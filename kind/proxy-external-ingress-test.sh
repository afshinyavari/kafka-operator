#!/bin/bash
# End-to-end test of KafkaProxy externalAccess.type=INGRESS (nginx-ingress ssl-passthrough).
#
# nginx-ingress with --enable-ssl-passthrough peeks the TLS SNI and forwards raw bytes to
# the backend Service. The proxy still runs sniHostIdentifiesNode and identifies brokers from
# the SNI hostname. Clients hit the nginx-ingress LB IP on port 9094 (we patched the LB
# Service to forward 9094 → 443 so the proxy's clientPort can stay at 9094 across all modes).
#
# Pre-requisites:
#   - mcs-setup complete
#   - setup-external.sh metallb && setup-external.sh ingress
#   - ingress-nginx-controller LB Service patched to expose port 9094

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
# 1. Patch KafkaProxy → INGRESS mode.
# ------------------------------------------------------------------------------
info "Patching ${PROXY_NAME} to externalAccess.type=INGRESS..."
kubectl --context "${CTX}" -n "${NS}" patch kafkacluster my-kafka --type merge -p "
spec:
  proxy:
    externalAccess:
      type: INGRESS
      advertisedHostTemplate: \"\${clusterId}.kafka.example.com\"
      ingress:
        ingressClassName: nginx
" >/dev/null

info "Waiting for operator to render sniHostIdentifiesNode + Ingress..."
for _ in {1..30}; do
  CFG=$(kubectl --context "${CTX}" -n "${NS}" get cm "${PROXY_NAME}-config" \
    -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -c "sniHostIdentifiesNode" || echo 0)
  ING=$(kubectl --context "${CTX}" -n "${NS}" get ingress "${PROXY_NAME}" 2>/dev/null | wc -l)
  [ "${CFG}" -gt 0 ] && [ "${ING}" -gt 1 ] && break
  sleep 2
done
[ "${CFG}" -gt 0 ] || fail "Kroxylicious config did not switch to sniHostIdentifiesNode"
[ "${ING}" -gt 1 ] || fail "Ingress was not created by the operator"

INGRESS_IP=$(kubectl --context "${CTX}" -n ingress-nginx get svc ingress-nginx-controller \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
[ -n "${INGRESS_IP}" ] || fail "nginx-ingress controller has no LB IP yet"
info "nginx-ingress IP: ${INGRESS_IP}"
info "Resolved hostname (per-cluster): bootstrap.${HOST} → ${INGRESS_IP}"

info "Restarting proxy so the new ConfigMap takes effect..."
kubectl --context "${CTX}" -n "${NS}" rollout restart deployment "${PROXY_NAME}" >/dev/null
kubectl --context "${CTX}" -n "${NS}" rollout status deployment "${PROXY_NAME}" --timeout=120s >/dev/null
sleep 3

# Allow nginx-ingress to pick up the new Ingress resource.
info "Waiting for nginx-ingress to load the Ingress rules..."
sleep 5

# ------------------------------------------------------------------------------
# 2. Extract certs, fetch JWT, build PKCS12 (same as the other tests).
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
# 3. Produce + consume — host overrides point all SNI hostnames at the nginx-ingress LB IP.
# ------------------------------------------------------------------------------
ADD_HOSTS=( --add-host "bootstrap.${HOST}:${INGRESS_IP}" )
# Post-Wave-4b: brokerNodeIdRanges are derived by the orchestrator from spec.clusters
# (clusterIndex * 1000 + brokerOrdinal). For the kind rig: 1 broker per cluster, 3 clusters.
NODE_IDS="0-0 1000-1000 2000-2000"
for range in ${NODE_IDS}; do
  start="${range%-*}"; end="${range#*-}"
  for (( id=start; id<=end; id++ )); do
    ADD_HOSTS+=( --add-host "broker-${id}.${HOST}:${INGRESS_IP}" )
  done
done

BOOTSTRAP="bootstrap.${HOST}:${CLIENT_PORT}"
info "Producing 3 messages via ${BOOTSTRAP} → ${INGRESS_IP}:9094 (nginx-ingress ssl-passthrough)..."
PRODUCE_OUT=$(docker run --rm --network kind --user 0:0 \
  "${ADD_HOSTS[@]}" \
  -v "${WORK_DIR}:/work" --entrypoint bash "${KAFKA_IMAGE}" -c "
    export KAFKA_HEAP_OPTS='-Xmx128m -Xms64m'
    export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///work/jwt-token'
    printf 'ing-${RUN_ID}-1\ning-${RUN_ID}-2\ning-${RUN_ID}-3\n' | timeout 30 \
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

COUNT=$(echo "${CONSUME_OUT}" | grep -c "ing-${RUN_ID}-" || true)
echo ""
echo "────────────────────────────────────────────────────────────────────────"
if [ "${COUNT}" -ge 3 ]; then
  pass "proxy-external-ingress-test: produced + consumed ${COUNT} messages via nginx-ingress"
else
  echo -e "${RED}[FAIL]${NC} expected 3 messages, got ${COUNT}"
  echo "Producer output:"; echo "${PRODUCE_OUT}" | tail -10 | sed 's/^/    /'
  echo "Consumer output:"; echo "${CONSUME_OUT}" | tail -10 | sed 's/^/    /'
  exit 1
fi
