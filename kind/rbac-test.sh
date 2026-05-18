#!/bin/bash
# End-to-end Kafka RBAC test via SASL/OAUTHBEARER through the Kroxylicious proxy.
# Prerequisites: mcs-setup complete, keycloak-setup complete, proxy-setup complete.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
BOOTSTRAP="kafka-proxy.${NS}.svc.cluster.local:9094"
DIRECT_BOOTSTRAP="brokers-a-headless.${NS}.svc.cluster.local:9092"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET="rbac-proxy-secret"
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

echo ""
echo "══ Pre-flight: create topics via direct PLAINTEXT (bypasses proxy) ══"
for topic in orders orders-dlq invoices invoices-dlq; do
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- \
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${DIRECT_BOOTSTRAP}" \
      --create --if-not-exists \
      --topic "${topic}" \
      --partitions 1 --replication-factor 1 2>/dev/null \
    && echo "  topic ${topic}: ready" || echo "  topic ${topic}: already exists"
done

# Fetch JWT for a user via curl and write to shared token file inside the broker pod,
# then attempt to produce through the proxy.
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

  # Step 2: write client properties — file:// token URL (avoids HTTP endpoint restrictions)
  local cfg="/tmp/client-${user}.properties"
  kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    printf '%s\n' \
      'security.protocol=SASL_PLAINTEXT' \
      'sasl.mechanism=OAUTHBEARER' \
      'sasl.oauthbearer.token.endpoint.url=file://${TOKEN_FILE}' \
      'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
      'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
      > ${cfg}
  " 2>/dev/null

  # Step 3: produce — KAFKA_OPTS allows the file:// URL
  local output rc=0
  output=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
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
