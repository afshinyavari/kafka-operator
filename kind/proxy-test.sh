#!/bin/bash
# Quick end-to-end sanity check: produce/consume through Kroxylicious proxy using mTLS + OIDC.
# Uses alice's JWT (orders-team) and the operator-generated test client cert for mTLS.
# Prerequisites: mcs-setup complete (Keycloak, KafkaRbac, and KafkaProxy all deployed).
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
PROXY_BOOTSTRAP="kafka-proxy.${NS}.svc.cluster.local:9094"
CLIENT_SECRET="kafka-proxy-test-client-tls"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
TOKEN_FILE="/tmp/proxy-test-token"
# alice is in orders-team — RBAC only permits the 'orders'/'orders-dlq' topics.
# The authorizer matches consumer-group names against the same name list, so the
# consumer group must also be 'orders' to pass GroupResource authorization.
TOPIC="orders"
GROUP="orders"
RUN_ID="$(date +%s)"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }

BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: no brokers-a pod found on ${CTX}"; exit 1; }
info "Using broker pod: ${BROKER_POD}"

# Extract client cert, key, CA from secret (base64-encoded — safe to embed in bash -c strings)
CA_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.ca\.crt}')
CERT_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.crt}')
KEY_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.key}')

info "Setting up mTLS client cert and PKCS12 keystores inside broker pod..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  mkdir -p /tmp/proxy-test
  echo '${CA_B64}'   | base64 -d > /tmp/proxy-test/ca.crt
  echo '${CERT_B64}' | base64 -d > /tmp/proxy-test/client.crt
  echo '${KEY_B64}'  | base64 -d > /tmp/proxy-test/client.key
  rm -f /tmp/proxy-test/keystore.p12 /tmp/proxy-test/truststore.p12
  openssl pkcs12 -export \
    -inkey /tmp/proxy-test/client.key \
    -in    /tmp/proxy-test/client.crt \
    -out   /tmp/proxy-test/keystore.p12 \
    -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca \
    -file /tmp/proxy-test/ca.crt \
    -keystore /tmp/proxy-test/truststore.p12 \
    -storetype PKCS12 -storepass changeit 2>/dev/null
" 2>/dev/null

# Keycloak access tokens are short-lived (~minutes). Re-fetch immediately before
# each Kafka operation so the token is still valid when the proxy validates it.
fetch_token() {
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
      > '${TOKEN_FILE}'
  " 2>/dev/null
}

info "Fetching alice's JWT from Keycloak..."
fetch_token

info "Writing SASL_SSL client config..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'ssl.keystore.type=PKCS12' \
    'ssl.keystore.location=/tmp/proxy-test/keystore.p12' \
    'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' \
    'ssl.truststore.location=/tmp/proxy-test/truststore.p12' \
    'ssl.truststore.password=changeit' \
    'sasl.mechanism=OAUTHBEARER' \
    'sasl.oauthbearer.token.endpoint.url=file://${TOKEN_FILE}' \
    'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
    'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
    > /tmp/proxy-test/sasl-ssl.properties
" 2>/dev/null

echo ""
info "Pre-flight: creating topic '${TOPIC}' via broker SSL (super.user, bypasses proxy)..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
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
info "Producing 3 messages to '${TOPIC}' through proxy (mTLS + OIDC, SASL_SSL)..."
fetch_token
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  printf 'proxytest-${RUN_ID}-1\nproxytest-${RUN_ID}-2\nproxytest-${RUN_ID}-3\n' | timeout 20 \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${PROXY_BOOTSTRAP}' \
      --topic '${TOPIC}' \
      --producer.config /tmp/proxy-test/sasl-ssl.properties 2>/dev/null
" 2>/dev/null

echo ""
info "Consuming from '${TOPIC}' (group '${GROUP}') through proxy (mTLS + OIDC, SASL_SSL)..."
fetch_token
OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  timeout 25 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server '${PROXY_BOOTSTRAP}' \
    --topic '${TOPIC}' \
    --group '${GROUP}' \
    --from-beginning --timeout-ms 15000 \
    --consumer.config /tmp/proxy-test/sasl-ssl.properties 2>/dev/null
" 2>/dev/null)

# Count only messages this run produced (topic 'orders' is shared/persistent across runs)
COUNT=$(echo "${OUT}" | grep -c "proxytest-${RUN_ID}-" 2>/dev/null || true)

echo ""
echo "────────────────────────────────────────────────────────────────────────"
if [ "${COUNT}" -ge 3 ]; then
  echo -e "${GREEN}proxy-test PASSED${NC}: produced and consumed ${COUNT} messages via mTLS + OIDC proxy"
else
  echo -e "${RED}proxy-test FAILED${NC}: expected 3 messages, got output:"
  echo "${OUT}"
  exit 1
fi
