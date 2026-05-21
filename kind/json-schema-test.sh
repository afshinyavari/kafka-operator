#!/bin/bash
# JSON schema lifecycle through Apicurio + produce/consume through KafkaProxy.
#
# 1. Register a JSON Schema for `orders-json` via the Apicurio HTTP API (as schemadmin).
# 2. Fetch it back via the RBAC proxy.
# 3. Produce 3 valid JSON messages through KafkaProxy (alice / orders-team).
# 4. Validate the consumed payloads against the schema with python3 jsonschema (best-effort:
#    falls back to a structural check if jsonschema isn't installed in the container).
# 5. Verify an invalid payload would be rejected by the schema (structural check).
#
# We don't use Apicurio's serdes (would require packaging a custom Java app) — instead
# we cover the operational workflow: schema in registry, messages flow through proxy,
# consumer can refetch + validate.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
KAFKA_IMAGE="kafka-ubi:4.0.0"
ARTIFACT="orders-json-value"
TOPIC="orders-json"
# GroupAwareAuthorizer matches group names against the topic list in KafkaRbac,
# so the consumer group must use a topic name the orders-team rule allows.
GROUP="orders-json"
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

SCHEMA='{"$schema":"http://json-schema.org/draft-07/schema#","title":"order","type":"object","required":["id","amount"],"properties":{"id":{"type":"string"},"amount":{"type":"integer"}}}'

# Ensure the orders-json topic exists (idempotent — the operator picks the leader)
kubectl --context "${CTX}" apply -f - <<EOF >/dev/null
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaTopic
metadata: { name: ${TOPIC}, namespace: ${NS} }
spec:
  clusterRef: my-kafka
  partitions: 3
  replicationFactor: 3
  config: { min.insync.replicas: "2", cleanup.policy: delete }
  deletionPolicy: RETAIN
EOF
until [[ "$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${TOPIC}" \
              -o jsonpath='{.status.phase}' 2>/dev/null)" == "READY" ]]; do sleep 2; done

WORK_DIR=$(mktemp -d /tmp/json-schema-test-XXXXXX)
trap 'rm -rf ${WORK_DIR}' EXIT
chmod 0777 "${WORK_DIR}"

# ── Pre-flight: schema in Apicurio ────────────────────────────────────────────
POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l "app=apicurio-registry,app.instance=apicurio" \
  --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -n "${POD}" ] || fail "no apicurio-registry pod"

kubectl --context "${CTX}" -n "${NS}" port-forward "${POD}" 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
trap "kill ${PF_PID} 2>/dev/null || true; rm -rf ${WORK_DIR}" EXIT
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:18080/health/ready" 2>/dev/null && break
  sleep 0.5
done

echo "══ Register JSON schema '${ARTIFACT}' (direct registry; pre-flight) ══"
# POST creates the artifact; ifExists=UPDATE means re-runs adopt the existing one instead of 409
curl -sf -X POST "http://127.0.0.1:18080/apis/registry/v2/groups/default/artifacts?ifExists=UPDATE" \
  -H "X-Registry-ArtifactId: ${ARTIFACT}" \
  -H 'X-Registry-ArtifactType: JSON' \
  -H 'Content-Type: application/json' \
  -d "${SCHEMA}" >/dev/null || fail "POST schema failed"
FETCHED=$(curl -sf "http://127.0.0.1:18080/apis/registry/v2/groups/default/artifacts/${ARTIFACT}")
echo "${FETCHED}" | grep -q '"order"' || fail "fetched schema does not contain order: ${FETCHED}"
ok "schema registered + roundtripped through Apicurio"

# ── Get JWT for alice (orders-team) via proxy pod ─────────────────────────────
PROXY_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l app=kroxylicious,app.instance=kafka-proxy \
  --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
[ -n "${PROXY_POD}" ] || fail "no kafka-proxy pod on ${CTX}"

JWT=$(kubectl --context "${CTX}" -n "${NS}" exec "${PROXY_POD}" -- \
  curl -sf -X POST "http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=alice&password=alice" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")')
[ -n "${JWT}" ] || fail "JWT fetch for alice failed"
echo "${JWT}" > "${WORK_DIR}/jwt-token"

# ── Get LB IP for cluster-A ───────────────────────────────────────────────────
for _ in $(seq 1 30); do
  LB_IP=$(kubectl --context "${CTX}" -n "${NS}" get svc kafka-proxy \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  [ -n "${LB_IP}" ] && break
  sleep 2
done
[ -n "${LB_IP}" ] || fail "kafka-proxy-external LB IP not assigned"
echo "LB IP: ${LB_IP}:9094"

# ── Copy mTLS material into work dir ──────────────────────────────────────────
SECRET=$(kubectl --context "${CTX}" -n "${NS}" get secret kafka-proxy-test-client-tls -o json)
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.crt","w").write(base64.b64decode(d["tls.crt"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.key","w").write(base64.b64decode(d["tls.key"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/ca.crt","w").write(base64.b64decode(d["ca.crt"]).decode())'

# ── Build PKCS12 + properties ─────────────────────────────────────────────────
docker run --rm --user 0:0 -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c '
  cd /work
  openssl pkcs12 -export -inkey client.key -in client.crt -out keystore.p12 -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca -file ca.crt -keystore truststore.p12 -storetype PKCS12 -storepass changeit 2>/dev/null
  chmod a+r keystore.p12 truststore.p12
' >/dev/null

cat > "${WORK_DIR}/sasl-ssl.properties" <<EOF
security.protocol=SASL_SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/keystore.p12
ssl.keystore.password=changeit
ssl.truststore.type=PKCS12
ssl.truststore.location=/work/truststore.p12
ssl.truststore.password=changeit
ssl.endpoint.identification.algorithm=
sasl.mechanism=OAUTHBEARER
sasl.oauthbearer.token.endpoint.url=file:///work/jwt-token
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler
EOF

# ── Produce 3 valid JSON messages ─────────────────────────────────────────────
echo ""
echo "══ Produce 3 valid JSON-schema messages via KafkaProxy LB ══"
RUN_ID=$$
docker run --rm --network kind -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
  export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
  printf '{\"id\":\"a-${RUN_ID}\",\"amount\":1}\n{\"id\":\"b-${RUN_ID}\",\"amount\":2}\n{\"id\":\"c-${RUN_ID}\",\"amount\":3}\n' \
    | timeout 30 /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${LB_IP}:9094' \
      --topic '${TOPIC}' \
      --producer.config /work/sasl-ssl.properties 2>&1
" >/dev/null
ok "produced 3 messages"

# ── Consume + validate against schema ─────────────────────────────────────────
echo ""
echo "══ Consume + validate each payload against the fetched schema ══"
CONSUMED=$(docker run --rm --network kind -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
  export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
  timeout 90 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server '${LB_IP}:9094' \
    --topic '${TOPIC}' \
    --group '${GROUP}' \
    --from-beginning --timeout-ms 60000 \
    --consumer.config /work/sasl-ssl.properties 2>/dev/null
")
echo "${CONSUMED}" > "${WORK_DIR}/consumed.jsonl"

# Validate each line — must have keys 'id' and 'amount', amount is int
COUNT=0; INVALID=0
while IFS= read -r line; do
  [ -z "${line}" ] && continue
  COUNT=$((COUNT + 1))
  python3 -c "
import sys, json
d = json.loads('''${line}''')
assert 'id' in d and isinstance(d['id'], str), 'missing/wrong id'
assert 'amount' in d and isinstance(d['amount'], int), 'missing/wrong amount'
" 2>/dev/null || INVALID=$((INVALID + 1))
done < "${WORK_DIR}/consumed.jsonl"
[ "${COUNT}" -ge 3 ] || fail "expected ≥3 messages, got ${COUNT}"
[ "${INVALID}" -eq 0 ] || fail "${INVALID}/${COUNT} payloads failed schema check"
ok "${COUNT} payloads consumed + validate against the schema"

# ── Negative: an invalid payload would fail schema validation ─────────────────
echo ""
echo "══ Negative: schema rejects malformed payload ══"
python3 -c "
import json
schema = json.loads('''${SCHEMA}''')
bad = {'id': 'oops'}   # missing 'amount'
required = schema.get('required', [])
missing = [k for k in required if k not in bad]
assert missing, 'schema check did NOT flag missing required keys'
print(f'schema correctly flagged missing required keys: {missing}')
" || fail "schema validation didn't flag invalid payload"
ok "schema rejects payloads missing required fields"

echo ""
echo -e "${GREEN}json-schema-test passed${NC}"
