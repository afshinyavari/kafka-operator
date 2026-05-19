#!/bin/bash
# End-to-end test of the Apicurio schema-registry RecordValidation filter.
# Registers a JSON schema via the apicurio-rbac-proxy (with alice's JWT — also
# exercises the rbac-proxy auth path), then drives the schema-producer helper
# JAR (V3-enveloped records) to assert valid passes + invalid is rejected.
# Prerequisites: mcs-setup complete (Keycloak, Apicurio, KafkaProxy READY).
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CTX="kind-kafka-a"
NS="kafka"
PROXY_BOOTSTRAP="kafka-proxy.${NS}.svc.cluster.local:9094"
DIRECT_BOOTSTRAP="brokers-a-headless.${NS}.svc.clusterset.local:9092"
RBAC_PROXY="http://apicurio-rbac-proxy.${NS}.svc.cluster.local:8082"
APICURIO_URL="http://apicurio-registry.${NS}.svc.cluster.local:8080/apis/registry/v2"
CLIENT_SECRET="kafka-proxy-test-client-tls"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
TOKEN_FILE="/tmp/schema-validation-token"
TOPIC="orders-json"
ARTIFACT_ID="${TOPIC}-value"
JAR_HOST="${OPERATOR_DIR}/test-clients/schema-producer/target/schema-producer.jar"
JAR_POD="/tmp/schema-producer.jar"
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

# ── Build the schema-producer fat JAR if missing or stale ────────────────────
if [ ! -f "${JAR_HOST}" ] || [ "${OPERATOR_DIR}/test-clients/schema-producer/src" -nt "${JAR_HOST}" ]; then
  info "Building schema-producer fat JAR..."
  (cd "${OPERATOR_DIR}/test-clients/schema-producer" && mvn package -DskipTests -q)
fi
info "schema-producer.jar: $(du -h "${JAR_HOST}" | cut -f1)"

# ── Pre-flight: create test topic via broker SSL super.user ───────────────────
info "Pre-flight: creating '${TOPIC}' via broker SSL..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SSL' \
    'ssl.keystore.type=PKCS12' 'ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12' 'ssl.keystore.password=changeit' \
    'ssl.truststore.type=PKCS12' 'ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12' 'ssl.truststore.password=changeit' \
    'ssl.endpoint.identification.algorithm=' > /tmp/admin.properties
  export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${DIRECT_BOOTSTRAP}' --create --if-not-exists \
    --topic '${TOPIC}' --partitions 1 --replication-factor 1 --command-config /tmp/admin.properties 2>/dev/null
" 2>/dev/null

# ── mTLS keystore for the producer JAR ────────────────────────────────────────
CA_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.ca\.crt}')
CERT_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.crt}')
KEY_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" -o jsonpath='{.data.tls\.key}')

info "Setting up mTLS client cert + PKCS12 keystores inside broker pod..."
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  mkdir -p /tmp/schema-test
  echo '${CA_B64}'   | base64 -d > /tmp/schema-test/ca.crt
  echo '${CERT_B64}' | base64 -d > /tmp/schema-test/client.crt
  echo '${KEY_B64}'  | base64 -d > /tmp/schema-test/client.key
  rm -f /tmp/schema-test/keystore.p12 /tmp/schema-test/truststore.p12
  openssl pkcs12 -export -inkey /tmp/schema-test/client.key -in /tmp/schema-test/client.crt \
    -out /tmp/schema-test/keystore.p12 -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca -file /tmp/schema-test/ca.crt \
    -keystore /tmp/schema-test/truststore.p12 -storetype PKCS12 -storepass changeit 2>/dev/null
" 2>/dev/null

fetch_token() {
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
      > '${TOKEN_FILE}'
  " 2>/dev/null
}

# ── Copy the producer JAR into the broker pod (one-shot per test run) ────────
info "Copying schema-producer.jar into broker pod..."
kubectl --context "${CTX}" -n "${NS}" cp "${JAR_HOST}" "${BROKER_POD}:${JAR_POD}" 2>/dev/null

# ── Register the JSON Schema via the rbac-proxy (with alice's JWT) ────────────
info "Registering JSON Schema '${ARTIFACT_ID}' via rbac-proxy (alice JWT)..."
fetch_token
SCHEMA_JSON='{"$schema":"http://json-schema.org/draft-07/schema#","type":"object","required":["id","amount"],"properties":{"id":{"type":"string"},"amount":{"type":"number"}}}'
GLOBAL_ID=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  TOKEN=\$(cat ${TOKEN_FILE})
  curl -sf -XPOST '${RBAC_PROXY}/apis/registry/v2/groups/default/artifacts' \
    -H \"Authorization: Bearer \$TOKEN\" \
    -H 'X-Registry-ArtifactId: ${ARTIFACT_ID}' \
    -H 'X-Registry-ArtifactType: JSON' \
    -H 'Content-Type: application/json' \
    --data-raw '${SCHEMA_JSON}' \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d[\"globalId\"], end=\"\")'
" 2>&1)
if ! [[ "${GLOBAL_ID}" =~ ^[0-9]+$ ]]; then
  echo "Registration response: ${GLOBAL_ID}"
  fail "Schema registration via rbac-proxy did not return a globalId"
fi
ok "Registered JSON Schema globalId=${GLOBAL_ID}"

# ── Test 1: VALID record (Apicurio serializer emits V3 envelope) → should pass
info "Producing VALID JSON record via schema-producer.jar..."
fetch_token
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  cat > /tmp/schema-test/valid.json <<'EOF'
{\"id\":\"ORD-${RUN_ID}-OK\",\"amount\":42.5}
EOF
" 2>/dev/null
VALID_OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
  java -jar ${JAR_POD} \
    --mode valid --bootstrap '${PROXY_BOOTSTRAP}' --topic '${TOPIC}' \
    --token-file ${TOKEN_FILE} \
    --keystore /tmp/schema-test/keystore.p12 \
    --truststore /tmp/schema-test/truststore.p12 \
    --apicurio-url '${APICURIO_URL}' \
    --payload-file /tmp/schema-test/valid.json 2>&1
" 2>&1) || { echo "${VALID_OUT}"; fail "VALID record produce failed unexpectedly"; }
if echo "${VALID_OUT}" | grep -q "^OK:"; then
  ok "VALID record accepted: $(echo "${VALID_OUT}" | grep '^OK:' | head -1)"
else
  echo "${VALID_OUT}"
  fail "VALID producer did not print OK"
fi

# ── Test 2: INVALID record (schema-violating payload + valid envelope) → reject
info "Producing INVALID JSON record (envelope refers to globalId=${GLOBAL_ID}, payload violates schema)..."
fetch_token
kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  cat > /tmp/schema-test/invalid.json <<'EOF'
{\"id\":\"ORD-${RUN_ID}-BAD\",\"amount\":\"not-a-number\"}
EOF
" 2>/dev/null
INVALID_OUT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
  java -jar ${JAR_POD} \
    --mode invalid --bootstrap '${PROXY_BOOTSTRAP}' --topic '${TOPIC}' \
    --token-file ${TOKEN_FILE} \
    --keystore /tmp/schema-test/keystore.p12 \
    --truststore /tmp/schema-test/truststore.p12 \
    --apicurio-url '${APICURIO_URL}' \
    --global-id ${GLOBAL_ID} \
    --payload-file /tmp/schema-test/invalid.json 2>&1
" 2>&1) || true
if echo "${INVALID_OUT}" | grep -qiE "REJECTED|INVALID_RECORD|RecordValidation|schema validation"; then
  ok "INVALID record rejected by RecordValidation filter"
else
  echo "${INVALID_OUT}"
  fail "INVALID record was not rejected as expected"
fi

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${GREEN}schema-validation-test PASSED${NC}: Apicurio RecordValidation filter accepts valid + rejects invalid"
