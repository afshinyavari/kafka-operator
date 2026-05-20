#!/bin/bash
# End-to-end schema registry RBAC test via Apicurio RBAC proxy.
# Prerequisites: keycloak-setup complete, proxy-setup complete, apicurio-setup complete.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
# The registry has no in-cluster Service — only the rbac-proxy is exposed. For the pre-flight
# (which seeds artifacts directly into the registry, bypassing authz) we port-forward the
# registry container locally and talk to it from the host.
REGISTRY_LOCAL_PORT="18080"
REGISTRY_DIRECT="http://127.0.0.1:${REGISTRY_LOCAL_PORT}"
REGISTRY_PROXY="http://apicurio-rbac-proxy.${NS}.svc.cluster.local:8082"
KEYCLOAK_TOKEN_URL="http://keycloak.${NS}.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token"
CLIENT_ID="rbac-proxy"
CLIENT_SECRET="rbac-proxy-secret"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
pass=0; fail=0

ok()     { echo -e "  ${GREEN}PASS${NC}  $*"; pass=$((pass+1)); }
fail_t() { echo -e "  ${RED}FAIL${NC}  $*"; fail=$((fail+1)); }

BROKER_POD=$(kubectl --context "${CTX}" get pods -n "${NS}" \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)
[ -n "${BROKER_POD}" ] || { echo "ERROR: No broker pod found on ${CTX}"; exit 1; }
echo "Using pod: ${BROKER_POD}"

SCHEMA='{"type":"object","properties":{"id":{"type":"string"}}}'

echo ""
echo "══ Pre-flight: register artifacts directly on registry (bypasses RBAC proxy) ══"
kubectl --context "${CTX}" -n "${NS}" port-forward deploy/apicurio-registry \
  "${REGISTRY_LOCAL_PORT}:8080" > /dev/null 2>&1 &
PF_PID=$!
trap 'kill ${PF_PID} 2>/dev/null || true' EXIT
# Wait up to 6s for the local port to start accepting connections
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "${REGISTRY_DIRECT}/health/ready" 2>/dev/null && break
  sleep 0.2
done
for artifact in orders invoices; do
  curl -sf -X POST "${REGISTRY_DIRECT}/apis/registry/v2/groups/default/artifacts" \
    -H "X-Registry-ArtifactId: ${artifact}" \
    -H 'X-Registry-ArtifactType: JSON' \
    -H 'Content-Type: application/json' \
    -d "${SCHEMA}" > /dev/null 2>&1 || true
  echo "  artifact '${artifact}': ready"
done
kill ${PF_PID} 2>/dev/null || true
trap - EXIT

# Fetch JWT for user and call RBAC proxy.
# method=GET → READ test; method=POST → WRITE test (posts a new version).
# Returns 0=2xx (allowed), 1=403 (denied), 2=unexpected.
try_schema() {
  local user="$1" artifact="$2" method="$3"

  local token
  token=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -X POST '${KEYCLOAK_TOKEN_URL}' \
      -d 'grant_type=password&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&username=${user}&password=${user}' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")'
  " 2>/dev/null)

  local url extra_flags=""
  if [ "${method}" = "GET" ]; then
    url="${REGISTRY_PROXY}/apis/registry/v2/groups/default/artifacts/${artifact}"
  else
    url="${REGISTRY_PROXY}/apis/registry/v2/groups/default/artifacts/${artifact}/versions"
    extra_flags="-H 'Content-Type: application/json' -d '${SCHEMA}'"
  fi

  local http_code rc=0
  http_code=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
    curl -sf -o /dev/null -w '%{http_code}' -X ${method} '${url}' \
      -H 'Authorization: Bearer ${token}' ${extra_flags} 2>/dev/null
  " 2>/dev/null) || rc=$?

  if [ "${http_code}" = "403" ]; then return 1; fi
  if echo "${http_code}" | grep -qE '^2'; then return 0; fi
  echo "  UNEXPECTED (${user} ${method} ${artifact}): HTTP ${http_code}" >&2
  return 2
}

echo ""
echo "══ alice (orders-team): READ+WRITE orders, DENIED on invoices ══"

try_schema alice orders GET && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "alice → orders (READ): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "alice → orders (READ): expected ALLOWED, got DENIED"
else                       fail_t "alice → orders (READ): unexpected error (see stderr)"; fi

try_schema alice orders POST && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "alice → orders (WRITE): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "alice → orders (WRITE): expected ALLOWED, got DENIED"
else                       fail_t "alice → orders (WRITE): unexpected error (see stderr)"; fi

try_schema alice invoices GET && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "alice → invoices (READ): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "alice → invoices (READ): expected DENIED, got ALLOWED"
else                       fail_t "alice → invoices (READ): unexpected error (see stderr)"; fi

try_schema alice invoices POST && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "alice → invoices (WRITE): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "alice → invoices (WRITE): expected DENIED, got ALLOWED"
else                       fail_t "alice → invoices (WRITE): unexpected error (see stderr)"; fi

echo ""
echo "══ bob (invoices-team): READ+WRITE invoices, DENIED on orders ══"

try_schema bob invoices GET && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "bob → invoices (READ): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "bob → invoices (READ): expected ALLOWED, got DENIED"
else                       fail_t "bob → invoices (READ): unexpected error (see stderr)"; fi

try_schema bob invoices POST && res=0 || res=$?
if   [ $res -eq 0 ]; then ok "bob → invoices (WRITE): ALLOWED"
elif [ $res -eq 1 ]; then fail_t "bob → invoices (WRITE): expected ALLOWED, got DENIED"
else                       fail_t "bob → invoices (WRITE): unexpected error (see stderr)"; fi

try_schema bob orders GET && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "bob → orders (READ): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "bob → orders (READ): expected DENIED, got ALLOWED"
else                       fail_t "bob → orders (READ): unexpected error (see stderr)"; fi

try_schema bob orders POST && res=0 || res=$?
if   [ $res -eq 1 ]; then ok "bob → orders (WRITE): DENIED (correct)"
elif [ $res -eq 0 ]; then fail_t "bob → orders (WRITE): expected DENIED, got ALLOWED"
else                       fail_t "bob → orders (WRITE): unexpected error (see stderr)"; fi

echo ""
echo "────────────────────────────────────────────────────────────────────────────"
total=$((pass+fail))
if [ "${fail}" -eq 0 ]; then
  echo -e "${GREEN}All ${total} schema registry RBAC tests passed.${NC}"
else
  echo -e "${RED}${fail}/${total} tests FAILED.${NC}"
  exit 1
fi
