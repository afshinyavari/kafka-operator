#!/bin/bash
# End-to-end test of KafkaProxy externalAccess.type=LOADBALANCER.
#
# Strategy: run kafka-console-producer/consumer in a transient docker container attached to
# the kind network. From the proxy's perspective this is "external" — not a pod in any of
# the Kafka clusters. The container hits the MetalLB-assigned LoadBalancer IP and relies on
# SASL_SSL (OIDC + mTLS), exactly the same way an outside-cluster client would.
#
# Pre-requisites:
#   - mcs-setup complete (KafkaProxy + KafkaRbac + Keycloak running)
#   - setup-external.sh metallb run (MetalLB allocates LB IPs on the kind bridge)
#   - operator reloaded with externalAccess support

set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
PROXY_NAME="kafka-proxy"
CLIENT_PORT=9094
KEYCLOAK_HOST="keycloak.${NS}.svc.clusterset.local"
CLIENT_SECRET="kafka-proxy-test-client-tls"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
TOPIC="orders"
GROUP="orders"
RUN_ID="$(date +%s)"
KAFKA_IMAGE="kafka-ubi:4.0.0"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }

# ------------------------------------------------------------------------------
# 1. Patch the KafkaProxy CR onto LOADBALANCER mode (idempotent) + read the assigned IP.
# ------------------------------------------------------------------------------
info "Patching ${PROXY_NAME} to externalAccess.type=LOADBALANCER..."
kubectl --context "${CTX}" -n "${NS}" patch kafkaproxy "${PROXY_NAME}" --type merge \
  -p '{"spec":{"externalAccess":{"type":"LOADBALANCER"}}}' >/dev/null

info "Waiting for MetalLB to assign a LoadBalancer ingress IP..."
LB_IP=""
for _ in {1..60}; do
  LB_IP=$(kubectl --context "${CTX}" -n "${NS}" get svc "${PROXY_NAME}" \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  [ -n "${LB_IP}" ] && break
  sleep 1
done
[ -n "${LB_IP}" ] || fail "LoadBalancer IP never assigned"
info "LB IP: ${LB_IP}"

info "Waiting for operator to rewrite advertisedBrokerAddressPattern with the LB IP..."
for _ in {1..30}; do
  PATTERN=$(kubectl --context "${CTX}" -n "${NS}" get cm "${PROXY_NAME}-config" \
    -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -o "advertisedBrokerAddressPattern: ${LB_IP}" || echo "")
  [ -n "${PATTERN}" ] && break
  sleep 2
done
[ -n "${PATTERN}" ] || fail "ConfigMap not updated with LB IP ${LB_IP}"

info "Restarting proxy so the new ConfigMap takes effect..."
kubectl --context "${CTX}" -n "${NS}" rollout restart deployment "${PROXY_NAME}" >/dev/null
kubectl --context "${CTX}" -n "${NS}" rollout status deployment "${PROXY_NAME}" --timeout=120s >/dev/null
sleep 2

# ------------------------------------------------------------------------------
# 2. Materialize mTLS client certs + SASL_SSL config on the host.
# ------------------------------------------------------------------------------
info "Extracting client mTLS cert from secret ${CLIENT_SECRET}..."
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.ca\.crt}'  | base64 -d > "${WORK_DIR}/ca.crt"
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > "${WORK_DIR}/client.crt"
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.key}' | base64 -d > "${WORK_DIR}/client.key"

info "Fetching alice's JWT from Keycloak (via cluster-A control-plane portfwd)..."
# Keycloak is only resolvable from inside the cluster (clusterset.local). Exec into the proxy
# pod just to fetch the token, then write it to the host workdir.
PROXY_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l app=kroxylicious \
  --field-selector=status.phase=Running \
  --no-headers -o custom-columns='NAME:.metadata.name' | head -1)
[ -n "${PROXY_POD}" ] || fail "no proxy pod found"
kubectl --context "${CTX}" -n "${NS}" exec "${PROXY_POD}" -- \
  curl -sf -X POST "http://${KEYCLOAK_HOST}:8080/realms/demo/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")' \
  > "${WORK_DIR}/jwt-token"
[ -s "${WORK_DIR}/jwt-token" ] || fail "JWT fetch failed"

# ------------------------------------------------------------------------------
# 3. Build PKCS12 keystores inside a transient docker container (host has no keytool).
# ------------------------------------------------------------------------------
info "Building PKCS12 keystores..."
chmod 0777 "${WORK_DIR}"
chmod 0666 "${WORK_DIR}"/*
docker run --rm --user 0:0 -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c '
  cd /work
  openssl pkcs12 -export \
    -inkey client.key -in client.crt \
    -out keystore.p12 -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca \
    -file ca.crt \
    -keystore truststore.p12 \
    -storetype PKCS12 -storepass changeit 2>/dev/null
  chmod a+r keystore.p12 truststore.p12
' 2>&1 | tail -3

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

# ------------------------------------------------------------------------------
# 4. Produce + consume from outside the cluster, against ${LB_IP}:${CLIENT_PORT}.
# ------------------------------------------------------------------------------
BOOTSTRAP="${LB_IP}:${CLIENT_PORT}"
info "Producing 3 messages to '${TOPIC}' via ${BOOTSTRAP} (external)..."
PRODUCE_OUT=$(docker run --rm --network kind \
  -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
    export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
    export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
    printf 'lb-${RUN_ID}-1\nlb-${RUN_ID}-2\nlb-${RUN_ID}-3\n' | timeout 30 \
      /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server '${BOOTSTRAP}' \
        --topic '${TOPIC}' \
        --producer.config /work/sasl-ssl.properties 2>&1
  " || true)
echo "${PRODUCE_OUT}" | grep -vE "^$" | tail -5 | sed 's/^/    /'

info "Consuming from '${TOPIC}' via ${BOOTSTRAP} (external)..."
CONSUME_OUT=$(docker run --rm --network kind \
  -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
    export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
    export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
    timeout 30 /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server '${BOOTSTRAP}' \
      --topic '${TOPIC}' \
      --group '${GROUP}' \
      --from-beginning --timeout-ms 20000 \
      --consumer.config /work/sasl-ssl.properties 2>/dev/null
  " || true)

COUNT=$(echo "${CONSUME_OUT}" | grep -c "lb-${RUN_ID}-" || true)
echo ""
echo "────────────────────────────────────────────────────────────────────────"
if [ "${COUNT}" -ge 3 ]; then
  pass "proxy-external-lb-test: produced + consumed ${COUNT} messages via ${BOOTSTRAP}"
else
  echo -e "${RED}[FAIL]${NC} expected 3 messages, got ${COUNT}"
  echo "Producer output:"; echo "${PRODUCE_OUT}" | tail -10 | sed 's/^/    /'
  echo "Consumer output:"; echo "${CONSUME_OUT}" | tail -10 | sed 's/^/    /'
  exit 1
fi
