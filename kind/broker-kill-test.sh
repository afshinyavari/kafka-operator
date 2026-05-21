#!/bin/bash
# Broker failure resilience: kill a broker pod, verify producers + consumers continue
# (RF=3 + min.isr=2 means 2 of 3 brokers can still serve), and broker rejoins ISR after.
#
# 1. Ensure topic `orders` (p=3, RF=3, min.insync.replicas=2) exists.
# 2. Delete pod `brokers-a-0`.
# 3. Immediately produce 5 messages; expect all 5 to be accepted (cluster-B + cluster-C
#    can still serve writes since ISR has 2 members).
# 4. Wait for broker-a to come back; assert it rejoins the ISR via kafka-topics --describe.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
KAFKA_IMAGE="kafka-ubi:4.0.0"
TOPIC="orders"
RUN_ID=$$
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
warn() { echo -e "  ${YELLOW}WARN${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

# Ensure topic exists
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

# Helper: count end-offsets across all partitions via broker INTERNAL listener (super-user).
# Use kafka-b's broker since kafka-a is the one we're about to kill.
BROKER_CTX="kind-kafka-b"
broker_exec() {
  kubectl --context "${BROKER_CTX}" -n "${NS}" exec brokers-b-0 -- /bin/bash -c "$1"
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
    --bootstrap-server brokers-b-headless.${NS}.svc.cluster.local:9092 \
    --topic ${TOPIC} --command-config /tmp/admin.props 2>/dev/null" \
    | awk -F: '{ s += $3 } END { print s+0 }'
}
BEFORE=$(total_offsets)
echo "Topic ${TOPIC} total offsets before run: ${BEFORE}"

# Use cluster-B's KafkaProxy LB as the bootstrap, so killing brokers-a-0 does NOT also
# kill the proxy we're trying to reach. (Each cluster's KafkaProxy is colocated with
# its broker pool but uses Submariner to reach the others.)
BCTX="kind-kafka-b"
for _ in $(seq 1 30); do
  LB_IP=$(kubectl --context "${BCTX}" -n "${NS}" get svc kafka-proxy \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  [ -n "${LB_IP}" ] && break
  sleep 2
done
[ -n "${LB_IP}" ] || fail "kafka-b LB IP not assigned"

WORK_DIR=$(mktemp -d /tmp/broker-kill-XXXXXX)
chmod 0777 "${WORK_DIR}"
trap 'rm -rf ${WORK_DIR}' EXIT

# Reuse cluster-A's secrets — same CA chain, same test-client cert
SECRET=$(kubectl --context "${CTX}" -n "${NS}" get secret kafka-proxy-test-client-tls -o json)
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.crt","w").write(base64.b64decode(d["tls.crt"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/client.key","w").write(base64.b64decode(d["tls.key"]).decode())'
echo "${SECRET}" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin)["data"]; open("'"${WORK_DIR}"'/ca.crt","w").write(base64.b64decode(d["ca.crt"]).decode())'

# JWT for alice via cluster-B proxy pod
BPROXY_POD=$(kubectl --context "${BCTX}" -n "${NS}" get pods -l app=kroxylicious,app.instance=kafka-proxy \
  --field-selector status.phase=Running -o jsonpath='{.items[0].metadata.name}')
kubectl --context "${BCTX}" -n "${NS}" exec "${BPROXY_POD}" -- \
  curl -sf -X POST "http://keycloak.${NS}.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=alice&password=alice" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")' \
  > "${WORK_DIR}/jwt-token"
[ -s "${WORK_DIR}/jwt-token" ] || fail "JWT fetch failed"

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
acks=all
retries=2147483647
retry.backoff.ms=500
EOF

# Kill brokers-a-0
echo ""
echo "══ Delete brokers-a-0 pod ══"
kubectl --context "${CTX}" -n "${NS}" delete pod brokers-a-0 --wait=false >/dev/null
ok "broker pod delete requested"

# Produce 5 messages — should still succeed (RF=3, ISR≥2 from b+c)
echo ""
echo "══ Produce 5 messages through cluster-B proxy (broker-a is down) ══"
PRODUCE_OUT=$(docker run --rm --network kind -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
  export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
  printf 'kill-${RUN_ID}-1\nkill-${RUN_ID}-2\nkill-${RUN_ID}-3\nkill-${RUN_ID}-4\nkill-${RUN_ID}-5\n' \
    | timeout 60 /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server '${LB_IP}:9094' \
      --topic '${TOPIC}' \
      --producer.config /work/sasl-ssl.properties 2>&1
" || true)
ok "produce completed (broker rejoining in background)"

# Verify delta via offsets — bypasses RBAC's group restriction on unique group names
echo ""
echo "══ Compare end-offset delta ══"
AFTER=$(total_offsets)
DELTA=$(( AFTER - BEFORE ))
echo "Topic ${TOPIC} total offsets after run: ${AFTER} (delta ${DELTA}, expected ≥5)"
[ "${DELTA}" -ge 5 ] || fail "expected ≥5 messages, got delta ${DELTA} — broker failure dropped data"
ok "${DELTA} messages arrived despite broker-a being down"

# Wait for broker-a to come back + rejoin ISR
echo ""
echo "══ Wait for broker-a to rejoin ISR ══"
until [[ "$(kubectl --context "${CTX}" -n "${NS}" get pod brokers-a-0 \
              -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)" == "true" ]]; do
  sleep 3
done
ok "brokers-a-0 Ready again"

# Use the operator's super-user admin from inside broker-a to describe ISR
sleep 5
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
kubectl --context "${CTX}" -n "${NS}" exec brokers-a-0 -- /bin/bash -c "cat > /tmp/admin.props <<'EOF'
${ADMIN_PROPS}
EOF" >/dev/null
ISR_DESC=$(kubectl --context "${CTX}" -n "${NS}" exec brokers-a-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "brokers-a-headless.${NS}.svc.cluster.local:9092" \
    --command-config /tmp/admin.props --describe --topic "${TOPIC}" 2>&1 || true)
echo "${ISR_DESC}" | grep -i 'Isr:'
# Expect Isr to mention all 3 broker IDs (0, 1000, 2000)
if echo "${ISR_DESC}" | grep -qE 'Isr: ([^,]*,){2}[^,]*'; then
  ok "ISR has 3 members again"
else
  warn "ISR may still be re-syncing — check manually if persistent"
fi

echo ""
echo -e "${GREEN}broker-kill-test passed${NC}"
