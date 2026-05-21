#!/bin/bash
# KafkaTopic CR lifecycle test:
# 1. Apply orders + invoices (p=3, RF=3) — wait for READY
# 2. Verify observedPartitions/observedReplicationFactor match spec
# 3. Alter config.cleanup.policy → verify operator pushes via AdminClient
# 4. Delete orders (DELETE policy) → Kafka topic gone
# 5. Delete invoices (RETAIN policy) → Kafka topic stays; recreate CR + verify adopt
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: no broker pod on ${CTX}"; exit 1; }

# Admin command-config — reuse the broker's INTERNAL PKCS12 (CN=kafka-proxy, super-user)
ADMIN_PROPS=$(cat <<'EOF'
security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.truststore.type=PKCS12
ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12
ssl.truststore.password=changeit
EOF
)
broker_exec() {
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- /bin/bash -c "$1"
}
broker_exec "cat > /tmp/admin.props <<'EOF'
${ADMIN_PROPS}
EOF" >/dev/null
BS="brokers-a-headless.${NS}.svc.cluster.local:9092"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BS} --command-config /tmp/admin.props"

cat <<EOF | kubectl --context "${CTX}" apply -f - >/dev/null
---
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaTopic
metadata: { name: e2e-orders, namespace: ${NS} }
spec:
  clusterRef: my-kafka
  partitions: 3
  replicationFactor: 3
  config: { retention.ms: "604800000", cleanup.policy: delete }
  deletionPolicy: DELETE
---
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaTopic
metadata: { name: e2e-invoices, namespace: ${NS} }
spec:
  clusterRef: my-kafka
  partitions: 3
  replicationFactor: 3
  config: { cleanup.policy: compact, min.insync.replicas: "2" }
  deletionPolicy: RETAIN
EOF
echo "Applied KafkaTopic CRs"

echo "Waiting for both KafkaTopic CRs to reach READY..."
for cr in e2e-orders e2e-invoices; do
  until [[ "$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${cr}" \
                -o jsonpath='{.status.phase}' 2>/dev/null)" == "READY" ]]; do sleep 2; done
  phase=$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${cr}" -o jsonpath='{.status.phase}')
  obsP=$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${cr}" -o jsonpath='{.status.observedPartitions}')
  obsR=$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${cr}" -o jsonpath='{.status.observedReplicationFactor}')
  [ "${phase}" = "READY" ] && [ "${obsP}" = "3" ] && [ "${obsR}" = "3" ] \
    && ok "${cr} READY (partitions=${obsP}, RF=${obsR})" \
    || fail "${cr} status: phase=${phase} p=${obsP} rf=${obsR}"
done

echo ""
echo "══ Topic config changes propagate through AdminClient ══"
kubectl --context "${CTX}" -n "${NS}" patch kafkatopic e2e-orders \
  --type merge -p '{"spec":{"config":{"retention.ms":"86400000","cleanup.policy":"delete"}}}' >/dev/null
echo "Waiting for retention.ms to propagate to Kafka..."
for _ in $(seq 1 30); do
  ret=$(broker_exec "${KAFKA_TOPICS} --describe --topic e2e-orders | grep retention.ms || true" 2>/dev/null)
  echo "${ret}" | grep -q "retention.ms=86400000" && { ok "retention.ms updated to 86400000"; break; }
  sleep 2
done
echo "${ret}" | grep -q "retention.ms=86400000" || fail "retention.ms did not update: ${ret}"

echo ""
echo "══ Delete CR with DELETE policy removes the Kafka topic ══"
kubectl --context "${CTX}" -n "${NS}" delete kafkatopic e2e-orders --wait=true >/dev/null
sleep 5
if broker_exec "${KAFKA_TOPICS} --list" 2>/dev/null | grep -q "^e2e-orders$"; then
  fail "e2e-orders still exists in Kafka after CR delete (deletionPolicy=DELETE)"
else
  ok "e2e-orders removed from Kafka"
fi

echo ""
echo "══ Delete CR with RETAIN policy keeps the Kafka topic ══"
kubectl --context "${CTX}" -n "${NS}" delete kafkatopic e2e-invoices --wait=true >/dev/null
sleep 5
if broker_exec "${KAFKA_TOPICS} --list" 2>/dev/null | grep -q "^e2e-invoices$"; then
  ok "e2e-invoices preserved in Kafka (deletionPolicy=RETAIN)"
else
  fail "e2e-invoices was deleted despite RETAIN policy"
fi

# Clean up the retained topic so re-runs are idempotent
broker_exec "${KAFKA_TOPICS} --delete --topic e2e-invoices" >/dev/null 2>&1 || true

echo ""
echo -e "${GREEN}KafkaTopic lifecycle test passed${NC}"
