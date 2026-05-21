#!/bin/bash
# End-to-end test of the custom XML validation filter on the Kroxylicious proxy.
# Registers an XSD via the filter's HTTP API, then produces valid + invalid XML
# through the proxy (SASL_SSL + OIDC) and asserts pass/reject behavior.
# Prerequisites: mcs-setup complete (Keycloak, Apicurio, KafkaProxy READY).
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
PROXY_BOOTSTRAP="kafka-proxy.${NS}.svc.cluster.local:9094"
DIRECT_BOOTSTRAP="brokers-a-headless.${NS}.svc.clusterset.local:9092"
CLIENT_SECRET="kafka-proxy-test-client-tls"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
TOKEN_FILE="/tmp/xml-filter-token"
TOPIC="orders-xml"
SCHEMA_TOPIC="xml-schemas"
RUN_ID="$(date +%s)"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: no brokers-a pod found on ${CTX}"; exit 1; }
info "Using broker pod: ${BROKER_POD}"

PROXY_POD=$(kubectl --context "${CTX}" -n "${NS}" get pod -l app=kroxylicious \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -n "${PROXY_POD}" ] || { echo "ERROR: no proxy pod found on ${CTX}"; exit 1; }
info "Using proxy pod:  ${PROXY_POD}"

# ── mTLS keystore (extracts the operator-provisioned test-client cert) ────────
CA_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.ca\.crt}')
CERT_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.crt}')
KEY_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.key}')

info "Setting up mTLS client cert + PKCS12 keystores inside broker pod..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  mkdir -p /tmp/xml-test
  echo '${CA_B64}'   | base64 -d > /tmp/xml-test/ca.crt
  echo '${CERT_B64}' | base64 -d > /tmp/xml-test/client.crt
  echo '${KEY_B64}'  | base64 -d > /tmp/xml-test/client.key
  rm -f /tmp/xml-test/keystore.p12 /tmp/xml-test/truststore.p12
  openssl pkcs12 -export -inkey /tmp/xml-test/client.key -in /tmp/xml-test/client.crt \
    -out /tmp/xml-test/keystore.p12 -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca -file /tmp/xml-test/ca.crt \
    -keystore /tmp/xml-test/truststore.p12 -storetype PKCS12 -storepass changeit 2>/dev/null
" 2>/dev/null

fetch_token() {
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
      > '${TOKEN_FILE}'
  " 2>/dev/null
}

info "Writing SASL_SSL client config..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SASL_SSL' \
    'ssl.keystore.type=PKCS12' \
    'ssl.keystore.location=/tmp/xml-test/keystore.p12' \
    'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' \
    'ssl.truststore.location=/tmp/xml-test/truststore.p12' \
    'ssl.truststore.password=changeit' \
    'ssl.endpoint.identification.algorithm=' \
    'sasl.mechanism=OAUTHBEARER' \
    'sasl.oauthbearer.token.endpoint.url=file://${TOKEN_FILE}' \
    'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
    'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
    > /tmp/xml-test/sasl-ssl.properties
" 2>/dev/null

# ── Pre-flight: create the validated topic + the compacted schema topic ────────
info "Pre-flight: creating '${TOPIC}' and compacted '${SCHEMA_TOPIC}' via broker SSL (super.user)..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SSL' \
    'ssl.keystore.type=PKCS12' 'ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12' 'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' 'ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12' 'ssl.truststore.password=changeit' \
    'ssl.endpoint.identification.algorithm=' > /tmp/admin.properties
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${DIRECT_BOOTSTRAP}' --create --if-not-exists \
    --topic '${TOPIC}' --partitions 1 --replication-factor 3 --command-config /tmp/admin.properties 2>/dev/null
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${DIRECT_BOOTSTRAP}' --create --if-not-exists \
    --topic '${SCHEMA_TOPIC}' --partitions 1 --replication-factor 3 \
    --config cleanup.policy=compact --command-config /tmp/admin.properties 2>/dev/null
" 2>/dev/null

# ── Register the XSD via the XML filter's HTTP API on the proxy pod ───────────
info "Registering 'order' XSD for topic '${TOPIC}' via XML filter HTTP API (proxy pod localhost:8080)..."
ORDER_XSD='<?xml version="1.0" encoding="UTF-8"?><xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"><xs:element name="order"><xs:complexType><xs:sequence><xs:element name="id" type="xs:string"/><xs:element name="amount" type="xs:decimal"/></xs:sequence></xs:complexType></xs:element></xs:schema>'
SCHEMA_JSON=$(python3 -c "
import json
print(json.dumps({'topic': '${TOPIC}', 'xsd': '''${ORDER_XSD}'''}))
")
# Proxy pod has curl? Check; if not, fall back to wget or extract via kubectl.
kubectl --context "${CTX}" -n "${NS}" exec "${PROXY_POD}" -- sh -c "
  if command -v curl >/dev/null 2>&1; then
    curl -sf -XPOST -H 'Content-Type: application/json' -d '${SCHEMA_JSON//\'/\\\'}' http://localhost:8080/schemas
  else
    wget -qO- --header 'Content-Type: application/json' --post-data='${SCHEMA_JSON//\'/\\\'}' http://localhost:8080/schemas
  fi
" || fail "schema registration HTTP call failed"
echo ""
info "Waiting 3s for XmlSchemaStore to consume the schema record..."
sleep 3

# ── Test 1: VALID XML payload should pass through the filter ──────────────────
info "Producing VALID XML to '${TOPIC}' (alice, SASL_SSL)..."
fetch_token
VALID_OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  printf '<order><id>ORD-${RUN_ID}-1</id><amount>99.99</amount></order>\n' | timeout 20 \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${PROXY_BOOTSTRAP}' --topic '${TOPIC}' \
      --producer.config /tmp/xml-test/sasl-ssl.properties 2>&1
" 2>&1) || true

if echo "${VALID_OUT}" | grep -qiE "ERROR|INVALID_RECORD|Exception" | grep -ivE "Expiring|WARN"; then
  echo "${VALID_OUT}"
  fail "VALID XML produce was rejected unexpectedly"
fi
# Confirm the message actually landed via direct-SSL consume
fetch_token
FOUND=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  timeout 15 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server '${DIRECT_BOOTSTRAP}' --topic '${TOPIC}' \
    --from-beginning --timeout-ms 8000 \
    --consumer.config /tmp/admin.properties 2>/dev/null
" 2>&1 | grep -c "ORD-${RUN_ID}-1" || true)
[ "${FOUND}" -ge 1 ] && ok "VALID XML accepted by filter and reached broker"
[ "${FOUND}" -ge 1 ] || fail "VALID XML produce returned 0 but message not found at broker"

# ── Test 2: INVALID XML payload should be rejected by the filter ──────────────
info "Producing INVALID XML (missing <amount>) to '${TOPIC}'..."
fetch_token
INVALID_OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
  printf '<order><id>ORD-${RUN_ID}-BAD</id></order>\n' | timeout 20 \
    /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${PROXY_BOOTSTRAP}' --topic '${TOPIC}' \
      --producer.config /tmp/xml-test/sasl-ssl.properties 2>&1
" 2>&1) || true

if echo "${INVALID_OUT}" | grep -qiE "INVALID_RECORD|XML schema validation|schema validation failed"; then
  ok "INVALID XML rejected by filter (INVALID_RECORD)"
else
  # Some Kafka producers only log it; double-check the broker doesn't have it
  STILL_FOUND=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
    timeout 10 /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server '${DIRECT_BOOTSTRAP}' --topic '${TOPIC}' \
      --from-beginning --timeout-ms 5000 \
      --consumer.config /tmp/admin.properties 2>/dev/null
  " 2>&1 | grep -c "ORD-${RUN_ID}-BAD" || true)
  if [ "${STILL_FOUND}" -eq 0 ]; then
    ok "INVALID XML rejected (not present at broker)"
  else
    echo "${INVALID_OUT}"
    fail "INVALID XML was accepted and stored at broker"
  fi
fi

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${GREEN}xml-filter-test PASSED${NC}: XML validation filter accepts valid + rejects invalid"
