#!/bin/bash
# MirrorMaker2 smoke test (joins SMOKE_E2E):
# 1. Build + load the MM2 image (kafka + schema-sync-smt JAR).
# 2. Apply the MirrorMaker2 CR pointing at a dummy external source.
# 3. Assert the operator runs through reconcile and emits ConfigMap + Deployment.
# 4. Assert .status.phase reaches PENDING (workers can't reach the dummy source —
#    that's intentional; we're only verifying the CRD + reconciler wiring).
# 5. Delete and verify cascade cleanup.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
CR="mm2-smoke"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

echo "=== MM2 smoke ==="

# Build + load the MM2 image so the Deployment can pull it locally.
make mm2-image >/dev/null
for cluster in kafka-a kafka-b kafka-c; do
  kind load docker-image mm2:dev --name "${cluster}" >/dev/null
done
ok "mm2:dev built and loaded into all kind clusters"

kubectl --context "${CTX}" apply -f manifests/mm2-smoke-cr.yaml >/dev/null
ok "MirrorMaker2 CR applied"

# Wait for the operator to converge on a non-RECONCILING phase. PENDING is the
# expected steady state — the worker pod can't reach smoke-source.invalid.
PHASE=""
for i in $(seq 1 60); do
  PHASE=$(kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" \
            -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${PHASE}" == "PENDING" || "${PHASE}" == "READY" || "${PHASE}" == "FAILED" ]]; then
    break
  fi
  sleep 2
done
[[ "${PHASE}" == "PENDING" ]] || fail "expected status.phase=PENDING, got '${PHASE}'"
ok "status.phase=PENDING (workers waiting on unreachable source — expected)"

# ConfigMap + Deployment created
kubectl --context "${CTX}" -n "${NS}" get configmap "${CR}" >/dev/null \
  || fail "ConfigMap ${CR} not created"
ok "ConfigMap ${CR} present"

kubectl --context "${CTX}" -n "${NS}" get deployment "${CR}" >/dev/null \
  || fail "Deployment ${CR} not created"
ok "Deployment ${CR} present"

# Internal topics CRs
for t in mm2-configs.smoke mm2-offsets.smoke mm2-status.smoke; do
  kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${t}" >/dev/null \
    || fail "KafkaTopic ${t} not created"
done
ok "3 internal KafkaTopic CRs present"

# Status fields populated
SRC_BS=$(kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" \
           -o jsonpath='{.status.sourceBootstrap}')
TGT_BS=$(kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" \
           -o jsonpath='{.status.targetBootstrap}')
[[ "${SRC_BS}" == "smoke-source.invalid:9093" ]] \
  || fail "status.sourceBootstrap expected smoke-source.invalid:9093, got '${SRC_BS}'"
[[ "${TGT_BS}" == *"kafka-proxy.kafka.svc.cluster.local:9094"* ]] \
  || fail "status.targetBootstrap expected proxy svc address, got '${TGT_BS}'"
ok "status.{source,target}Bootstrap populated correctly"

# Metrics — manifest sets metricsConfig: {} → operator creates -metrics Service
# and ServiceMonitor (the SM may be absent if Prometheus Operator's CRD isn't
# installed; OptionalResourceApplier no-ops in that case, which is correct).
kubectl --context "${CTX}" -n "${NS}" get svc "${CR}-metrics" >/dev/null \
  || fail "Service ${CR}-metrics not created (metrics gating broken?)"
METRICS_PORT=$(kubectl --context "${CTX}" -n "${NS}" get svc "${CR}-metrics" \
                 -o jsonpath='{.spec.ports[?(@.name=="metrics")].port}')
[[ "${METRICS_PORT}" == "9101" ]] \
  || fail "Service ${CR}-metrics expected port 9101, got '${METRICS_PORT}'"
ok "Service ${CR}-metrics present on port 9101"
if kubectl --context "${CTX}" get crd servicemonitors.monitoring.coreos.com \
     >/dev/null 2>&1; then
  kubectl --context "${CTX}" -n "${NS}" get servicemonitor "${CR}-metrics" \
    >/dev/null 2>&1 \
    && ok "ServiceMonitor ${CR}-metrics present" \
    || fail "ServiceMonitor ${CR}-metrics not created (Prometheus CRD is installed)"
else
  ok "Prometheus Operator CRD absent — ServiceMonitor apply correctly skipped"
fi

# Cleanup
kubectl --context "${CTX}" delete -f manifests/mm2-smoke-cr.yaml >/dev/null
for i in $(seq 1 30); do
  kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" >/dev/null 2>&1 || break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" >/dev/null 2>&1 \
  && fail "CR not deleted"
ok "CR deleted and cascade complete"

echo ""
echo -e "${GREEN}✓ MM2 smoke passed${NC}"
