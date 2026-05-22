#!/bin/bash
# cruise-control smoke test (joins SMOKE_E2E):
#  1. Build + load the Cruise Control image into all kind clusters.
#  2. Enable spec.cruiseControl on KafkaCluster my-kafka — assert the operator deploys
#     Cruise Control and reports status.cruiseControl.phase=READY.
#  3. Apply a KafkaRebalance CR — assert the reconciler drives Cruise Control and the CR
#     reaches PENDING_PROPOSAL / PROPOSAL_READY.
#  4. Assert the rebalance does NOT auto-execute without the approve annotation.
#  5. Delete the KafkaRebalance CR.
#
# This verifies CRD + reconciler + image wiring. A full proposal -> approve -> execute
# round trip (Cruise Control needs minutes of metric windows) is the user-run extended
# test. Cruise Control is left enabled on my-kafka so re-runs are idempotent.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() {
  echo -e "  ${RED}FAIL${NC}  $*"
  kubectl --context "${CTX}" -n "${NS}" get kafkacluster my-kafka \
    -o jsonpath='{.status.cruiseControl}' 2>/dev/null || true
  echo ""
  kubectl --context "${CTX}" -n "${NS}" get kafkarebalance cc-rebalance-smoke \
    -o jsonpath='{.status}' 2>/dev/null || true
  echo ""
  exit 1
}

echo "=== cruise-control smoke ==="

# Build + load the Cruise Control image so the Deployment can pull it locally.
make reload-cruise-control-image >/dev/null
ok "cruise-control image built and loaded into all kind clusters"

# Idempotent re-run: drop any leftover KafkaRebalance from a prior run.
kubectl --context "${CTX}" -n "${NS}" delete kafkarebalance cc-rebalance-smoke \
  --ignore-not-found >/dev/null 2>&1 || true

# ── Enable Cruise Control ────────────────────────────────────────────────────
# Also set spec.metricsConfig: presence (without configMapRef) leaves broker JMX
# disabled but turns on the operator-bundled metrics path for the proxy + CC.
kubectl --context "${CTX}" -n "${NS}" patch kafkacluster my-kafka --type merge \
  -p '{"spec":{"cruiseControl":{},"metricsConfig":{}}}' >/dev/null
ok "spec.cruiseControl + spec.metricsConfig enabled on KafkaCluster my-kafka"

CCPHASE=""
for i in $(seq 1 90); do
  CCPHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkacluster my-kafka \
            -o jsonpath='{.status.cruiseControl.phase}' 2>/dev/null || true)
  if [[ "${CCPHASE}" == "READY" || "${CCPHASE}" == "FAILED" ]]; then
    break
  fi
  sleep 4
done
[[ "${CCPHASE}" == "READY" ]] || fail "expected status.cruiseControl.phase=READY, got '${CCPHASE}'"
ok "Cruise Control deployed (status.cruiseControl.phase=READY)"

kubectl --context "${CTX}" -n "${NS}" get deployment cruise-control >/dev/null 2>&1 \
  || fail "cruise-control Deployment not created"
kubectl --context "${CTX}" -n "${NS}" get svc cruise-control >/dev/null 2>&1 \
  || fail "cruise-control Service not created"
ok "cruise-control Deployment + Service present"

# ── Metrics — gated on spec.metricsConfig ────────────────────────────────────
# CC: dedicated cruise-control-metrics Service (9101) + ServiceMonitor when the
# Prometheus Operator CRD is present.
for i in $(seq 1 30); do
  kubectl --context "${CTX}" -n "${NS}" get svc cruise-control-metrics \
    >/dev/null 2>&1 && break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get svc cruise-control-metrics >/dev/null 2>&1 \
  || fail "cruise-control-metrics Service not created"
CC_METRICS_PORT=$(kubectl --context "${CTX}" -n "${NS}" get svc cruise-control-metrics \
                    -o jsonpath='{.spec.ports[?(@.name=="metrics")].port}')
[[ "${CC_METRICS_PORT}" == "9101" ]] \
  || fail "cruise-control-metrics expected port 9101, got '${CC_METRICS_PORT}'"
ok "cruise-control-metrics Service present on port 9101"

# Proxy: kafka-proxy-metrics on 9190 (Kroxylicious native /metrics)
kubectl --context "${CTX}" -n "${NS}" get svc kafka-proxy-metrics >/dev/null 2>&1 \
  || fail "kafka-proxy-metrics Service not created"
PROXY_METRICS_PORT=$(kubectl --context "${CTX}" -n "${NS}" get svc kafka-proxy-metrics \
                       -o jsonpath='{.spec.ports[?(@.name=="metrics")].port}')
[[ "${PROXY_METRICS_PORT}" == "9190" ]] \
  || fail "kafka-proxy-metrics expected port 9190, got '${PROXY_METRICS_PORT}'"
ok "kafka-proxy-metrics Service present on port 9190"

if kubectl --context "${CTX}" get crd servicemonitors.monitoring.coreos.com \
     >/dev/null 2>&1; then
  kubectl --context "${CTX}" -n "${NS}" get servicemonitor cruise-control-metrics \
    >/dev/null 2>&1 \
    && kubectl --context "${CTX}" -n "${NS}" get servicemonitor kafka-proxy-metrics \
      >/dev/null 2>&1 \
    && ok "ServiceMonitors cruise-control-metrics + kafka-proxy-metrics present" \
    || fail "ServiceMonitor(s) missing despite Prometheus CRD installed"
else
  ok "Prometheus Operator CRD absent — ServiceMonitor apply correctly skipped"
fi

# ── KafkaRebalance proposal ──────────────────────────────────────────────────
kubectl --context "${CTX}" apply -f manifests/kafka-rebalance-cr.yaml >/dev/null
ok "KafkaRebalance CR applied"

RPHASE=""
for i in $(seq 1 60); do
  RPHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkarebalance cc-rebalance-smoke \
           -o jsonpath='{.status.phase}' 2>/dev/null || true)
  case "${RPHASE}" in
    PENDING_PROPOSAL|PROPOSAL_READY|NOT_READY|REBALANCING|READY) break ;;
  esac
  sleep 3
done
case "${RPHASE}" in
  PENDING_PROPOSAL|PROPOSAL_READY)
    ok "KafkaRebalance reached ${RPHASE} — reconciler drove Cruise Control" ;;
  *)
    fail "expected KafkaRebalance PENDING_PROPOSAL/PROPOSAL_READY, got '${RPHASE}'" ;;
esac

# The proposal must NOT execute without the approve annotation.
[[ "${RPHASE}" != "REBALANCING" && "${RPHASE}" != "READY" ]] \
  || fail "KafkaRebalance executed without the approve annotation"
ok "KafkaRebalance did not auto-execute without approval"

# ── Cleanup (Cruise Control stays enabled — re-runs are idempotent) ──────────
kubectl --context "${CTX}" -n "${NS}" delete kafkarebalance cc-rebalance-smoke >/dev/null
ok "KafkaRebalance CR deleted"

echo ""
echo -e "${GREEN}✓ cruise-control smoke passed${NC}"
