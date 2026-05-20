#!/bin/bash
# End-to-end check that the KafkaProxy is reachable across MCS clusters.
# Today the canonical proxy-test.sh runs entirely from cluster-a — both client and proxy
# are local, so it only exercises the single-cluster path. This script runs the same
# produce/consume but from cluster-b's broker pod, bootstrapping against the clusterset
# DNS name. Submariner routes the bootstrap to a proxy somewhere in the clusterset
# (local-preferred when one is exported on cluster-b), and the proxy's METADATA response
# now advertises per-broker addresses under `*.svc.clusterset.local` (the change in
# KroxyliciousConfigBuilder gated on spec.mcs.enabled) so the second round-trip resolves
# cross-cluster too.
#
# Prerequisites: `make mcs-setup` complete with PROXY_CLUSTERS=(kafka-a kafka-b),
# i.e. proxies READY on both kafka-a and kafka-b.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

# Run from cluster-b — the proxy is reached via Submariner.
CTX="${CTX:-kind-kafka-b}"
NS="kafka"
# clusterset.local — Submariner-routed across MCS clusters.
PROXY_BOOTSTRAP="${PROXY_BOOTSTRAP:-kafka-proxy.${NS}.svc.clusterset.local:9094}"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
CLIENT_SECRET="kafka-proxy-test-client-tls"
TOKEN_FILE="/tmp/mcs-proxy-test-token"
TOPIC="orders"
# RBAC pins the consumer group name to the same set as topic names (orders-team has access
# to groups "orders", "orders-dlq", etc.). Reuse "orders" — combined with --from-beginning
# and short --timeout-ms we read all messages regardless of committed offset.
GROUP="orders"
RUN_ID="$(date +%s)"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }

# Pick any broker pod on the client cluster — we only need a JVM with kafka clients.
BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: no broker pod found on ${CTX}"; exit 1; }
info "Client broker pod (in ${CTX}): ${BROKER_POD}"
info "Proxy bootstrap: ${PROXY_BOOTSTRAP}"

# Confirm the proxy pod actually exists locally on this cluster — that's what makes
# Submariner's local-preferred routing the meaningful first hop. (Failover to a remote
# cluster is covered by the second test in this script.)
LOCAL_PROXY_PODS=$(kubectl --context "${CTX}" -n "${NS}" get pods \
  -l app=kroxylicious,app.instance=kafka-proxy --no-headers 2>/dev/null | wc -l)
info "Local proxy pods on ${CTX}: ${LOCAL_PROXY_PODS} (clusterset routes here first when >0)"

CA_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.ca\.crt}')
CERT_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.crt}')
KEY_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.key}')

info "Setting up mTLS PKCS12 keystores inside ${BROKER_POD}..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  mkdir -p /tmp/mcs-proxy-test
  echo '${CA_B64}'   | base64 -d > /tmp/mcs-proxy-test/ca.crt
  echo '${CERT_B64}' | base64 -d > /tmp/mcs-proxy-test/client.crt
  echo '${KEY_B64}'  | base64 -d > /tmp/mcs-proxy-test/client.key
  rm -f /tmp/mcs-proxy-test/keystore.p12 /tmp/mcs-proxy-test/truststore.p12
  openssl pkcs12 -export \
    -inkey /tmp/mcs-proxy-test/client.key \
    -in    /tmp/mcs-proxy-test/client.crt \
    -out   /tmp/mcs-proxy-test/keystore.p12 \
    -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca \
    -file /tmp/mcs-proxy-test/ca.crt \
    -keystore /tmp/mcs-proxy-test/truststore.p12 \
    -storetype PKCS12 -storepass changeit 2>/dev/null
" 2>/dev/null

fetch_token() {
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
      > '${TOKEN_FILE}'
  " 2>/dev/null
}

info "Fetching alice's JWT from Keycloak (via clusterset.local)..."
fetch_token

info "Writing SASL_SSL client config inside the pod..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'ssl.keystore.type=PKCS12' \
    'ssl.keystore.location=/tmp/mcs-proxy-test/keystore.p12' \
    'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' \
    'ssl.truststore.location=/tmp/mcs-proxy-test/truststore.p12' \
    'ssl.truststore.password=changeit' \
    'sasl.mechanism=OAUTHBEARER' \
    'sasl.oauthbearer.token.endpoint.url=file://${TOKEN_FILE}' \
    'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
    'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
    > /tmp/mcs-proxy-test/sasl-ssl.properties
" 2>/dev/null

# Pre-flight topic creation: topic 'orders' may not exist if rbac-test never ran.
# We use the broker SSL (super.user) channel that bypasses the proxy — same shape as
# proxy-test.sh's pre-flight.
info "Pre-flight: ensuring topic '${TOPIC}' exists via cluster-A broker..."
ADMIN_CTX="kind-kafka-a"
ADMIN_BROKER=$(kubectl --context "${ADMIN_CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
kubectl --context "${ADMIN_CTX}" -n "${NS}" exec "${ADMIN_BROKER}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SSL' \
    'ssl.keystore.type=PKCS12' \
    'ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12' \
    'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' \
    'ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12' \
    'ssl.truststore.password=changeit' \
    'ssl.endpoint.identification.algorithm=' \
    > /tmp/admin.properties
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server 'brokers-a-headless.${NS}.svc.clusterset.local:9092' \
    --create --if-not-exists \
    --topic '${TOPIC}' \
    --partitions 1 --replication-factor 1 \
    --command-config /tmp/admin.properties 2>/dev/null
" 2>/dev/null

echo ""
info "Producing 3 messages to '${TOPIC}' through MCS-routed proxy..."
fetch_token
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  printf 'mcstest-${RUN_ID}-1\nmcstest-${RUN_ID}-2\nmcstest-${RUN_ID}-3\n' | timeout 30 \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${PROXY_BOOTSTRAP}' \
      --topic '${TOPIC}' \
      --producer.config /tmp/mcs-proxy-test/sasl-ssl.properties 2>/dev/null
" 2>/dev/null

echo ""
info "Consuming from '${TOPIC}' through MCS-routed proxy..."
fetch_token
OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  timeout 30 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server '${PROXY_BOOTSTRAP}' \
    --topic '${TOPIC}' \
    --group '${GROUP}' \
    --from-beginning --timeout-ms 15000 \
    --consumer.config /tmp/mcs-proxy-test/sasl-ssl.properties 2>/dev/null
" 2>/dev/null)

COUNT=$(echo "${OUT}" | grep -c "mcstest-${RUN_ID}-" 2>/dev/null || true)

echo ""
echo "────────────────────────────────────────────────────────────────────────"
if [ "${COUNT}" -ge 3 ]; then
  echo -e "${GREEN}mcs-proxy-test PASSED${NC}: produced and consumed ${COUNT} messages via clusterset.local proxy bootstrap (client in ${CTX})"
else
  echo -e "${RED}mcs-proxy-test FAILED${NC}: expected 3 messages, got output:"
  echo "${OUT}"
  exit 1
fi
