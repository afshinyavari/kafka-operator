#!/bin/bash
# KafkaConnect + KafkaConnector smoke test (joins SMOKE_E2E):
# 1. Build + load the connect image and the operator image.
# 2. Apply the KafkaConnect CR attached to the managed `my-kafka` cluster.
# 3. Assert the operator emits ConfigMap, Deployment, REST Service, internal-topic CRs.
# 4. Wait for status.phase=READY.
# 5. Apply a KafkaConnector CR (FileStreamSourceConnector — bundled with stock Kafka).
# 6. Assert the operator pushes the config to REST and reports a Ready/Reconciling
#    phase with the connector visible.
# 7. Pause → resume → status reflects spec.state.
# 8. groupId collision: a second KafkaConnect with the same explicit groupId on the
#    same Kafka cluster stays FAILED.
# 9. Cleanup with cascade.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
KCC="kc-smoke"
KCON="kc-smoke-source"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

echo "=== KafkaConnect smoke ==="

# Build + load the connect image so the worker Deployment can pull it locally.
make connect-image >/dev/null
for cluster in kafka-a kafka-b kafka-c; do
  kind load docker-image connect:dev --name "${cluster}" >/dev/null
done
ok "connect:dev built and loaded into all kind clusters"

kubectl --context "${CTX}" apply -f manifests/kafka-connect-smoke-cr.yaml >/dev/null
ok "KafkaConnect CR applied"

# Wait for the operator to emit the ConfigMap + Deployment + REST Service.
for i in $(seq 1 30); do
  if kubectl --context "${CTX}" -n "${NS}" get configmap "${KCC}" >/dev/null 2>&1 \
     && kubectl --context "${CTX}" -n "${NS}" get deployment "${KCC}" >/dev/null 2>&1 \
     && kubectl --context "${CTX}" -n "${NS}" get svc "${KCC}-connect" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get configmap "${KCC}" >/dev/null \
  || fail "ConfigMap ${KCC} not created"
ok "ConfigMap ${KCC} present"
kubectl --context "${CTX}" -n "${NS}" get deployment "${KCC}" >/dev/null \
  || fail "Deployment ${KCC} not created"
ok "Deployment ${KCC} present"
kubectl --context "${CTX}" -n "${NS}" get svc "${KCC}-connect" >/dev/null \
  || fail "Service ${KCC}-connect not created"
ok "Service ${KCC}-connect present"

# Internal topics
for t in "connect-configs.${KCC}" "connect-offsets.${KCC}" "connect-status.${KCC}"; do
  kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${t}" >/dev/null \
    || fail "KafkaTopic ${t} not created"
done
ok "3 internal KafkaTopic CRs present"

# Wait for READY (worker pod must come up against the proxy).
PHASE=""
for i in $(seq 1 90); do
  PHASE=$(kubectl --context "${CTX}" -n "${NS}" get kcc "${KCC}" \
            -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${PHASE}" == "READY" ]]; then break; fi
  if [[ "${PHASE}" == "FAILED" ]]; then
    kubectl --context "${CTX}" -n "${NS}" get kcc "${KCC}" -o yaml | tail -40
    fail "status.phase=FAILED unexpectedly"
  fi
  sleep 3
done
[[ "${PHASE}" == "READY" ]] || fail "expected status.phase=READY, got '${PHASE}'"
ok "KafkaConnect status.phase=READY"

# Status URL populated
URL=$(kubectl --context "${CTX}" -n "${NS}" get kcc "${KCC}" -o jsonpath='{.status.url}')
[[ "${URL}" == "http://${KCC}-connect.${NS}.svc.cluster.local:8083" ]] \
  || fail "status.url unexpected: ${URL}"
ok "status.url populated: ${URL}"

# Metrics
kubectl --context "${CTX}" -n "${NS}" get svc "${KCC}-metrics" >/dev/null \
  || fail "Service ${KCC}-metrics not created (metrics gating broken?)"
ok "Service ${KCC}-metrics present"

# Apply a connector CR
kubectl --context "${CTX}" apply -f manifests/kafka-connect-smoke-connector.yaml >/dev/null
ok "KafkaConnector CR applied"

# Wait for the operator to push the config to REST and report status.
CON_PHASE=""
for i in $(seq 1 60); do
  CON_PHASE=$(kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" \
                -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${CON_PHASE}" == "Ready" || "${CON_PHASE}" == "Reconciling" ]]; then
    # Ready = workers fully running tasks; Reconciling = still assigning.
    # Both prove the operator's REST plumbing works.
    break
  fi
  sleep 3
done
[[ "${CON_PHASE}" == "Ready" || "${CON_PHASE}" == "Reconciling" ]] \
  || fail "expected connector phase Ready or Reconciling, got '${CON_PHASE}'"
ok "KafkaConnector status.phase=${CON_PHASE}"

# observedConfigHash should be populated after first successful PUT/POST.
HASH=$(kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" \
         -o jsonpath='{.status.observedConfigHash}')
[[ -n "${HASH}" ]] || fail "status.observedConfigHash empty"
ok "status.observedConfigHash=${HASH}"

# Pause/resume
kubectl --context "${CTX}" -n "${NS}" patch kcon "${KCON}" --type=merge \
  -p '{"spec":{"state":"paused"}}' >/dev/null
for i in $(seq 1 30); do
  P=$(kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "${P}" == "Paused" ]] && break
  sleep 2
done
[[ "${P}" == "Paused" ]] || fail "expected Paused, got '${P}'"
ok "pause: status.phase=Paused"

kubectl --context "${CTX}" -n "${NS}" patch kcon "${KCON}" --type=merge \
  -p '{"spec":{"state":"running"}}' >/dev/null
for i in $(seq 1 30); do
  P=$(kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" \
        -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "${P}" == "Ready" || "${P}" == "Reconciling" ]] && break
  sleep 2
done
[[ "${P}" == "Ready" || "${P}" == "Reconciling" ]] || fail "expected Ready/Reconciling after resume, got '${P}'"
ok "resume: status.phase=${P}"

# groupId collision guard
cat <<EOF | kubectl --context "${CTX}" apply -f - >/dev/null
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaConnect
metadata:
  name: kc-smoke-conflict
  namespace: ${NS}
spec:
  image: connect:dev
  replicas: 1
  groupId: connect-kc-smoke
  kafkaClusterRef:
    kafkaClusterRef:
      name: my-kafka
  worker:
    internalReplicationFactor: 1
EOF
for i in $(seq 1 30); do
  CP=$(kubectl --context "${CTX}" -n "${NS}" get kcc kc-smoke-conflict \
         -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "${CP}" == "FAILED" ]] && break
  sleep 2
done
[[ "${CP}" == "FAILED" ]] || fail "groupId collision did not surface as FAILED, got '${CP}'"
MSG=$(kubectl --context "${CTX}" -n "${NS}" get kcc kc-smoke-conflict \
        -o jsonpath='{.status.message}')
echo "${MSG}" | grep -q "groupId" || fail "FAILED message missing 'groupId': ${MSG}"
ok "groupId collision guard surfaces phase=FAILED with groupId message"
kubectl --context "${CTX}" -n "${NS}" delete kcc kc-smoke-conflict >/dev/null

# Cleanup
kubectl --context "${CTX}" delete -f manifests/kafka-connect-smoke-connector.yaml >/dev/null
for i in $(seq 1 30); do
  kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" >/dev/null 2>&1 || break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get kcon "${KCON}" >/dev/null 2>&1 \
  && fail "KafkaConnector CR not deleted (finalizer stuck?)"
ok "KafkaConnector deleted (finalizer released)"

kubectl --context "${CTX}" delete -f manifests/kafka-connect-smoke-cr.yaml >/dev/null
for i in $(seq 1 30); do
  kubectl --context "${CTX}" -n "${NS}" get kcc "${KCC}" >/dev/null 2>&1 || break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get kcc "${KCC}" >/dev/null 2>&1 \
  && fail "KafkaConnect CR not deleted"
ok "KafkaConnect deleted and cascade complete"

echo ""
echo -e "${GREEN}✓ KafkaConnect smoke passed${NC}"
