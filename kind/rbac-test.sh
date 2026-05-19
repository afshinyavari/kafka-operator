#!/bin/bash
# End-to-end Kafka RBAC test via SASL/OAUTHBEARER + mTLS through the Kroxylicious proxy.
# All clients use SASL_SSL: mTLS transport (shared test-client cert) + JWT for group-based authorization.
# Prerequisites: mcs-setup complete (Keycloak, KafkaRbac, and KafkaProxy all deployed).
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
BOOTSTRAP="kafka-proxy.${NS}.svc.cluster.local:9094"
DIRECT_BOOTSTRAP="brokers-a-headless.${NS}.svc.clusterset.local:9092"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET="rbac-proxy-secret"
MTLS_SECRET="kafka-proxy-test-client-tls"
TOKEN_FILE="/tmp/kafka-oauth-token"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
pass=0; fail=0

ok()     { echo -e "  ${GREEN}PASS${NC}  $*"; pass=$((pass+1)); }
fail_t() { echo -e "  ${RED}FAIL${NC}  $*"; fail=$((fail+1)); }

# Find a broker pod dynamically
BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: No brokers-a pod found on ${CTX}"; exit 1; }
echo "Using broker pod: ${BROKER_POD}"

# ── mTLS setup: extract client cert from secret and create PKCS12 keystores ──
echo ""
echo "══ Setting up mTLS client cert (shared across all test users) ══"
CA_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${MTLS_SECRET}" \
  -o jsonpath='{.data.ca\.crt}')
CERT_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${MTLS_SECRET}" \
  -o jsonpath='{.data.tls\.crt}')
KEY_B64=$(kubectl --context "${CTX}" -n "${NS}" get secret "${MTLS_SECRET}" \
  -o jsonpath='{.data.tls\.key}')

kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  mkdir -p /tmp/rbac-test
  echo '${CA_B64}'   | base64 -d > /tmp/rbac-test/ca.crt
  echo '${CERT_B64}' | base64 -d > /tmp/rbac-test/client.crt
  echo '${KEY_B64}'  | base64 -d > /tmp/rbac-test/client.key
  rm -f /tmp/rbac-test/keystore.p12 /tmp/rbac-test/truststore.p12
  openssl pkcs12 -export \
    -inkey /tmp/rbac-test/client.key \
    -in    /tmp/rbac-test/client.crt \
    -out   /tmp/rbac-test/keystore.p12 \
    -passout pass:changeit 2>/dev/null
  keytool -importcert -noprompt -trustcacerts -alias ca \
    -file /tmp/rbac-test/ca.crt \
    -keystore /tmp/rbac-test/truststore.p12 \
    -storetype PKCS12 -storepass changeit 2>/dev/null
" 2>/dev/null
echo "  mTLS keystores ready"

echo ""
echo "══ Pre-flight: create topics via broker SSL (super.user, bypasses proxy) ══"
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
" 2>/dev/null
for topic in orders orders-dlq invoices invoices-dlq; do
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server '${DIRECT_BOOTSTRAP}' \
      --create --if-not-exists \
      --topic '${topic}' \
      --partitions 1 --replication-factor 1 \
      --command-config /tmp/admin.properties 2>/dev/null
  " 2>/dev/null \
    && echo "  topic ${topic}: ready" || echo "  topic ${topic}: already exists"
done

# Fetch JWT for a user via curl and write to shared token file inside the broker pod,
# then attempt to produce through the proxy using SASL_SSL (mTLS + OIDC).
# Returns 0=allowed, 1=RBAC denied, 2=unexpected error.
try_produce() {
  local user="$1" topic="$2"

  # Step 1: fetch JWT and write to token file (Kafka 4.0 FileTokenRetriever reads this)
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&username=${user}&password=${user}' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
      > '${TOKEN_FILE}'
  " 2>/dev/null

  # Step 2: write SASL_SSL client properties (mTLS + OAUTHBEARER)
  local cfg="/tmp/client-${user}.properties"
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    printf '%s\n' \
      'security.protocol=SASL_SSL' \
      'ssl.keystore.type=PKCS12' \
      'ssl.keystore.location=/tmp/rbac-test/keystore.p12' \
      'ssl.keystore.password=changeit' \
      'ssl.truststore.type=PKCS12' \
      'ssl.truststore.location=/tmp/rbac-test/truststore.p12' \
      'ssl.truststore.password=changeit' \
      'sasl.mechanism=OAUTHBEARER' \
      'sasl.oauthbearer.token.endpoint.url=file://${TOKEN_FILE}' \
      'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
      'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
      > ${cfg}
  " 2>/dev/null

  # Step 3: produce — KAFKA_OPTS allows the file:// URL; KAFKA_HEAP_OPTS avoids OOMKill
  local output rc=0
  output=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    export KAFKA_HEAP_OPTS='-Xmx64m -Xms32m'
    export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file://${TOKEN_FILE}'
    echo 'rbac-test-${user}-\$(date +%s)' | timeout 20 \
      /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server '${BOOTSTRAP}' \
        --topic '${topic}' \
        --producer.config '${cfg}' 2>&1
  " 2>/dev/null) || rc=$?

  if echo "${output}" | grep -qiE "TopicAuthorizationException|TOPIC_AUTHORIZATION_FAILED|Not authorized to access"; then
    return 1  # RBAC denied
  fi
  if [ $rc -ne 0 ] || echo "${output}" | grep -qiE "KafkaException|ConfigException|ERROR"; then
    echo "  UNEXPECTED ERROR (${user} -> ${topic}):" >&2
    echo "${output}" | head -10 >&2
    return 2  # infra/config error — fail loudly
  fi
  return 0  # allowed
}

echo ""
echo "══ alice (orders-team): ALLOWED on orders, DENIED on invoices ══"

try_produce alice orders && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "alice → orders (PRODUCE): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "alice → orders (PRODUCE): expected ALLOWED, got DENIED"
else                       fail_t "alice → orders (PRODUCE): unexpected error (see stderr)"; fi

try_produce alice invoices && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "alice → invoices (PRODUCE): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "alice → invoices (PRODUCE): expected DENIED, got ALLOWED"
else                       fail_t "alice → invoices (PRODUCE): unexpected error (see stderr)"; fi

echo ""
echo "══ bob (invoices-team): ALLOWED on invoices, DENIED on orders ══"

try_produce bob invoices && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "bob → invoices (PRODUCE): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "bob → invoices (PRODUCE): expected ALLOWED, got DENIED"
else                       fail_t "bob → invoices (PRODUCE): unexpected error (see stderr)"; fi

try_produce bob orders && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "bob → orders (PRODUCE): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "bob → orders (PRODUCE): expected DENIED, got ALLOWED"
else                       fail_t "bob → orders (PRODUCE): unexpected error (see stderr)"; fi

echo ""
echo "────────────────────────────────────────────────────────────────────────"
total=$((pass+fail))
if [ "${fail}" -eq 0 ]; then
  echo -e "${GREEN}All ${total} RBAC tests passed.${NC}"
else
  echo -e "${RED}${fail}/${total} tests FAILED.${NC}"
  exit 1
fi
