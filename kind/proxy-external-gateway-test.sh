#!/bin/bash
# End-to-end test of KafkaProxy externalAccess.type=GATEWAY (Gateway API + TLSRoute).
#
# The proxy runs in sniHostIdentifiesNode mode behind an Envoy Gateway with a TLS passthrough
# listener on port 9094. Clients connect to bootstrap.<host>:9094, the Gateway dispatches by
# SNI to the proxy Service, and the proxy uses the SNI hostname to identify the broker.
#
# We bypass real DNS by passing --add-host into the docker test client so the JVM resolves
# bootstrap.a.kafka.example.com (and the per-broker hostnames) to the Envoy LB IP.
#
# Pre-requisites:
#   - mcs-setup complete (KafkaProxy + KafkaRbac + Keycloak running)
#   - setup-external.sh metallb && setup-external.sh gateway (Envoy Gateway + MetalLB)
#   - operator reloaded with externalAccess support

set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
CLUSTER_ID="a"
NS="kafka"
PROXY_NAME="kafka-proxy"
CLIENT_PORT=9094
HOST="${CLUSTER_ID}.kafka.example.com"
KEYCLOAK_HOST="keycloak.${NS}.svc.clusterset.local"
CLIENT_SECRET="kafka-proxy-test-client-tls"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET_VAL="rbac-proxy-secret"
TOPIC="orders"
GROUP="orders"
RUN_ID="$(date +%s)"
KAFKA_IMAGE="kafka-ubi:4.0.0"
WORK_DIR="$(mktemp -d)"
chmod 0777 "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
fail() { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }
pass() { echo -e "${GREEN}[PASS]${NC}  $*"; }

# ------------------------------------------------------------------------------
# 1. Patch KafkaProxy → GATEWAY mode pointing at the kafka-gateway provisioned by setup-external.
# ------------------------------------------------------------------------------
info "Patching ${PROXY_NAME} to externalAccess.type=GATEWAY..."
kubectl --context "${CTX}" -n "${NS}" patch kafkacluster my-kafka --type merge -p "
spec:
  proxy:
    externalAccess:
      type: GATEWAY
      advertisedHostTemplate: \"\${clusterId}.kafka.example.com\"
      gateway:
        parentGatewayName: kafka-gateway
        parentGatewayNamespace: kafka
" >/dev/null

info "Waiting for operator to render sniHostIdentifiesNode + TLSRoute..."
for _ in {1..30}; do
  CFG=$(kubectl --context "${CTX}" -n "${NS}" get cm "${PROXY_NAME}-config" \
    -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -c "sniHostIdentifiesNode" || echo 0)
  ROUTE=$(kubectl --context "${CTX}" -n "${NS}" get tlsroute "${PROXY_NAME}" 2>/dev/null | wc -l)
  [ "${CFG}" -gt 0 ] && [ "${ROUTE}" -gt 1 ] && break
  sleep 2
done
[ "${CFG}" -gt 0 ] || fail "Kroxylicious config did not switch to sniHostIdentifiesNode"
[ "${ROUTE}" -gt 1 ] || fail "TLSRoute was not created by the operator"

GW_IP=$(kubectl --context "${CTX}" -n "${NS}" get gateway kafka-gateway \
  -o jsonpath='{.status.addresses[0].value}')
[ -n "${GW_IP}" ] || fail "Envoy Gateway has no address yet"
info "Envoy Gateway IP: ${GW_IP}"
info "Resolved hostname (per-cluster): bootstrap.${HOST} → ${GW_IP}"

info "Restarting proxy so the new ConfigMap takes effect..."
kubectl --context "${CTX}" -n "${NS}" rollout restart deployment "${PROXY_NAME}" >/dev/null
kubectl --context "${CTX}" -n "${NS}" rollout status deployment "${PROXY_NAME}" --timeout=120s >/dev/null
sleep 2

# Wait until the TLSRoute is Accepted by Envoy (otherwise the first connect 503s/RSTs).
info "Waiting for TLSRoute to become Accepted by Envoy..."
for _ in {1..30}; do
  ACCEPTED=$(kubectl --context "${CTX}" -n "${NS}" get tlsroute "${PROXY_NAME}" \
    -o jsonpath='{.status.parents[0].conditions[?(@.type=="Accepted")].status}' 2>/dev/null || echo "")
  [ "${ACCEPTED}" = "True" ] && break
  sleep 2
done
[ "${ACCEPTED}" = "True" ] || fail "TLSRoute never reached Accepted=True"

# ------------------------------------------------------------------------------
# 2. Extract certs, fetch JWT, build PKCS12 (same as proxy-external-lb-test.sh).
# ------------------------------------------------------------------------------
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.ca\.crt}'  | base64 -d > "${WORK_DIR}/ca.crt"
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > "${WORK_DIR}/client.crt"
kubectl --context "${CTX}" -n "${NS}" get secret "${CLIENT_SECRET}" \
  -o jsonpath='{.data.tls\.key}' | base64 -d > "${WORK_DIR}/client.key"

PROXY_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l app=kroxylicious \
  --field-selector=status.phase=Running \
  --no-headers -o custom-columns='NAME:.metadata.name' | head -1)
kubectl --context "${CTX}" -n "${NS}" exec "${PROXY_POD}" -- \
  curl -sf -X POST "http://${KEYCLOAK_HOST}:8080/realms/demo/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET_VAL}&username=alice&password=alice" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")' \
  > "${WORK_DIR}/jwt-token"
chmod 0666 "${WORK_DIR}"/*

info "Building PKCS12 keystores..."
docker run --rm --user 0:0 -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c '
  cd /work
  openssl pkcs12 -export -inkey client.key -in client.crt -out keystore.p12 -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca -file ca.crt \
    -keystore truststore.p12 -storetype PKCS12 -storepass changeit 2>/dev/null
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

# ------------------------------------------------------------------------------
# 3. Produce + consume — host overrides point bootstrap.<host> and broker-N.<host> at GW_IP.
# ------------------------------------------------------------------------------
ADD_HOSTS=( --add-host "bootstrap.${HOST}:${GW_IP}" )
# Post-Wave-4b: brokerNodeIdRanges are derived by the orchestrator from spec.clusters
# (clusterIndex * 1000 + brokerOrdinal). For the kind rig: 1 broker per cluster, 3 clusters.
NODE_IDS="0-0 1000-1000 2000-2000"
for range in ${NODE_IDS}; do
  start="${range%-*}"; end="${range#*-}"
  for (( id=start; id<=end; id++ )); do
    ADD_HOSTS+=( --add-host "broker-${id}.${HOST}:${GW_IP}" )
  done
done

BOOTSTRAP="bootstrap.${HOST}:${CLIENT_PORT}"
info "Producing 3 messages via ${BOOTSTRAP} → ${GW_IP} (Envoy Gateway, SNI passthrough)..."
PRODUCE_OUT=$(docker run --rm --network kind --user 0:0 \
  "${ADD_HOSTS[@]}" \
  -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
    export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
    export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
    printf 'gw-${RUN_ID}-1\ngw-${RUN_ID}-2\ngw-${RUN_ID}-3\n' | timeout 30 \
      /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server '${BOOTSTRAP}' \
        --topic '${TOPIC}' \
        --producer.config /work/sasl-ssl.properties 2>&1
  " || true)
echo "${PRODUCE_OUT}" | grep -vE "^$" | tail -5 | sed 's/^/    /'

info "Consuming from '${TOPIC}' via ${BOOTSTRAP}..."
CONSUME_OUT=$(docker run --rm --network kind --user 0:0 \
  "${ADD_HOSTS[@]}" \
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

COUNT=$(echo "${CONSUME_OUT}" | grep -c "gw-${RUN_ID}-" || true)
echo ""
echo "────────────────────────────────────────────────────────────────────────"
if [ "${COUNT}" -ge 3 ]; then
  pass "proxy-external-gateway-test: produced + consumed ${COUNT} messages via Envoy Gateway"
else
  echo -e "${RED}[FAIL]${NC} expected 3 messages, got ${COUNT}"
  echo "Producer output:"; echo "${PRODUCE_OUT}" | tail -10 | sed 's/^/    /'
  echo "Consumer output:"; echo "${CONSUME_OUT}" | tail -10 | sed 's/^/    /'
  exit 1
fi
