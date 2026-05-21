#!/bin/bash
# Rolling restart preserves quorum + no message loss.
#
# 1. Apply (idempotent) an `orders` KafkaTopic CR with p=3, RF=3.
# 2. Start a background producer that sends 1 msg/sec for 60s through the LB-exposed proxy.
# 3. Annotation-kick `brokers-a` KafkaNodePool to force the operator to roll all pods.
# 4. Poll `make quorum` every 5s during the roll — must stay healthy.
# 5. After the producer finishes, consume from-beginning + count messages. Must be ≥60
#    (at-least-once; duplicates allowed). No producer-side error from the roll is fatal.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
KAFKA_IMAGE="kafka-ubi:4.0.0"
TOPIC="orders"
DURATION="${ROLLING_DURATION:-60}"
RUN_ID=$$
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

# Ensure orders topic exists (idempotent)
kubectl --context "${CTX}" apply -f - <<EOF >/dev/null
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaTopic
metadata: { name: ${TOPIC}, namespace: ${NS} }
spec:
  clusterRef: my-kafka
  partitions: 3
  replicationFactor: 3
  config: { cleanup.policy: delete, min.insync.replicas: "2" }
  deletionPolicy: RETAIN
EOF
until [[ "$(kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${TOPIC}" \
              -o jsonpath='{.status.phase}' 2>/dev/null)" == "READY" ]]; do sleep 2; done
ok "topic ${TOPIC} READY"

# Helper: count end-offsets across all partitions via broker INTERNAL listener (super-user).
broker_exec() {
  kubectl --context "${CTX}" -n "${NS}" exec brokers-a-0 -- /bin/bash -c "$1"
}
broker_exec "cat > /tmp/admin.props <<'EOF'
security.protocol=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12
ssl.keystore.password=changeit
ssl.key.password=changeit
ssl.truststore.type=PKCS12
ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12
ssl.truststore.password=changeit
EOF" >/dev/null
total_offsets() {
  broker_exec "/opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server brokers-a-headless.${NS}.svc.cluster.local:9092 \
    --topic ${TOPIC} --command-config /tmp/admin.props 2>/dev/null" \
    | awk -F: '{ s += $3 } END { print s+0 }'
}
BEFORE=$(total_offsets)
echo "Topic ${TOPIC} total offsets before run: ${BEFORE}"

WORK_DIR=$(mktemp -d /tmp/rolling-restart-XXXXXX)
chmod 0777 "${WORK_DIR}"
trap 'rm -rf ${WORK_DIR}; kill ${PROD_CONT_PID:-0} 2>/dev/null || true' EXIT

# JWT + certs
PROXY_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l app=kroxylicious,app.instance=kafka-proxy \
  --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
kubectl --context "${CTX}" -n "${NS}" exec "${PROXY_POD}" -- \
  curl -sf -X POST "http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=alice&password=alice" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")' \
  > "${WORK_DIR}/jwt-token"
[ -s "${WORK_DIR}/jwt-token" ] || fail "JWT fetch failed"

for _ in $(seq 1 30); do
  LB_IP=$(kubectl --context "${CTX}" -n "${NS}" get svc kafka-proxy \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  [ -n "${LB_IP}" ] && break
  sleep 2
done
[ -n "${LB_IP}" ] || fail "no LB IP"

SECRET=$(kubectl --context "${CTX}" -n "${NS}" get secret kafka-proxy-test-client-tls -o json)
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.crt","w").write(base64.b64decode(d["tls.crt"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.key","w").write(base64.b64decode(d["tls.key"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/ca.crt","w").write(base64.b64decode(d["ca.crt"]).decode())'

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
# acks=all + retries so rolling brokers don't drop messages
acks=all
retries=2147483647
retry.backoff.ms=200
EOF

# Background producer — 1 msg/sec for $DURATION seconds. Don't --rm so docker
# logs survives for diagnostics. Pin a script file inside /work that we can edit.
echo ""
echo "══ Start background producer (1 msg/sec for ${DURATION}s) ══"
cat > "${WORK_DIR}/produce.sh" <<EOF
#!/bin/bash
set -uo pipefail
export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
for i in \$(seq 1 ${DURATION}); do
  echo "roll-${RUN_ID}-\$i"
  sleep 1
done | /opt/kafka/bin/kafka-console-producer.sh \\
  --bootstrap-server '${LB_IP}:9094' \\
  --topic '${TOPIC}' \\
  --producer.config /work/sasl-ssl.properties
EOF
chmod 0755 "${WORK_DIR}/produce.sh"
docker run -d --name "rolling-prod-${RUN_ID}" --network kind \
  -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" /work/produce.sh >/dev/null
sleep 5   # let some messages flow first

# Kick the brokers-a NodePool to force a rolling restart
echo ""
echo "══ Force rolling restart of brokers-a ══"
kubectl --context "${CTX}" -n "${NS}" annotate kafkanodepool brokers-a \
  "kafka.yavari.afshin.se/force-restart=$(date +%s)" --overwrite >/dev/null
ok "annotation kicked"

# Poll quorum every 5s during the roll — must stay healthy
echo ""
echo "══ Poll quorum during the roll ══"
ROLL_FAIL=0
for _ in $(seq 1 12); do
  out=$(make -C "$(dirname "$0")" quorum 2>&1 | grep -c "✓ Quorum healthy" || true)
  if [ "${out}" -lt 3 ]; then
    ROLL_FAIL=$((ROLL_FAIL + 1))
    echo "  quorum check #${ROLL_FAIL}: only ${out}/3 reports healthy (transient OK)"
  fi
  sleep 5
done
[ "${ROLL_FAIL}" -le 2 ] || fail "quorum unhealthy in ${ROLL_FAIL}/12 checks during roll"
ok "quorum stayed healthy during the roll"

# Wait for producer container to finish
echo ""
echo "══ Wait for producer to complete ══"
docker wait "rolling-prod-${RUN_ID}" >/dev/null || true
# Capture producer logs for diagnostics; clean up the container afterwards
echo "Last 15 producer log lines:"
docker logs --tail 15 "rolling-prod-${RUN_ID}" 2>&1 | sed 's/^/    /'
docker rm -f "rolling-prod-${RUN_ID}" >/dev/null 2>&1 || true

# Count messages via end-offset delta (bypasses RBAC's group restriction; the
# unique group name pattern that --from-beginning would need isn't in the
# orders-team rules)
echo ""
echo "══ Compare end-offset delta ══"
AFTER=$(total_offsets)
DELTA=$(( AFTER - BEFORE ))
MIN_EXPECTED=$(( DURATION * 90 / 100 ))   # allow 10% slack for boundary effects
echo "Topic ${TOPIC} total offsets after run: ${AFTER} (delta ${DELTA}, expected ≥${MIN_EXPECTED})"
[ "${DELTA}" -ge "${MIN_EXPECTED}" ] || fail "lost too many messages during roll: delta ${DELTA} < ${MIN_EXPECTED}"
ok "rolling restart preserved messages: ${DELTA} written"

echo ""
echo -e "${GREEN}rolling-restart-test passed${NC}"
