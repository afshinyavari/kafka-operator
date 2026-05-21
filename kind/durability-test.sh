#!/bin/bash
# kafkasql durability: register a schema via the Apicurio registry REST API, then
# delete one registry pod. After the deployment converges to 3/3 ready, fetch the
# schema back. If kafkasql is wired correctly (compacted journal topic, message
# survives JVM restart), the GET succeeds.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"
ARTIFACT="durability-test-$(date +%s)"
SCHEMA='{"type":"object","title":"durability","properties":{"id":{"type":"string"}}}'
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

POD=$(kubectl --context "${CTX}" -n "${NS}" get pods \
  -l "app=apicurio-registry,app.instance=apicurio" \
  --field-selector status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[ -n "${POD}" ] || fail "No running apicurio-registry pod found"

echo "Port-forwarding ${POD} :8080 → localhost:18080"
kubectl --context "${CTX}" -n "${NS}" port-forward "${POD}" 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
trap 'kill ${PF_PID} 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:18080/health/ready" 2>/dev/null && break
  sleep 0.5
done

echo ""
echo "══ Register schema '${ARTIFACT}' ══"
curl -sf -X POST "http://127.0.0.1:18080/apis/registry/v2/groups/default/artifacts" \
  -H "X-Registry-ArtifactId: ${ARTIFACT}" \
  -H 'X-Registry-ArtifactType: JSON' \
  -H 'Content-Type: application/json' \
  -d "${SCHEMA}" >/dev/null || fail "POST schema failed"
ok "schema registered"

# Stop the port-forward to a pod we're about to kill
kill ${PF_PID} 2>/dev/null || true
trap - EXIT

echo ""
echo "══ Delete pod ${POD} and wait for deployment to converge ══"
kubectl --context "${CTX}" -n "${NS}" delete pod "${POD}" --wait=false >/dev/null
until [[ "$(kubectl --context "${CTX}" -n "${NS}" get deployment apicurio-registry \
            -o jsonpath='{.status.readyReplicas}/{.spec.replicas}')" == "3/3" ]]; do
  sleep 3
done
ok "registry back to 3/3 ready"

echo ""
echo "══ Schema is still there after pod restart ══"
NEW_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods \
  -l "app=apicurio-registry,app.instance=apicurio" \
  --field-selector status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
kubectl --context "${CTX}" -n "${NS}" port-forward "${NEW_POD}" 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
trap 'kill ${PF_PID} 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -sf -o /dev/null "http://127.0.0.1:18080/health/ready" 2>/dev/null && break
  sleep 0.5
done
body=$(curl -sf "http://127.0.0.1:18080/apis/registry/v2/groups/default/artifacts/${ARTIFACT}" \
  || fail "GET schema after restart failed — kafkasql persistence broken")
echo "${body}" | grep -q '"durability"' || fail "Fetched schema does not match: ${body}"
ok "schema '${ARTIFACT}' survived pod restart"

echo ""
echo -e "${GREEN}Durability test passed${NC}"
