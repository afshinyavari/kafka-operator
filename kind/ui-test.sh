#!/bin/bash
# Smoke-test the kafka-ui Deployment (now backed by the kafka-editor image
# — same KafkaUI CRD, swapped image; see backlog #27).
#
# Verifies:
#  1. The pod is Ready (Quarkus reports /q/health/ready)
#  2. The home route redirects unauthenticated browsers to Keycloak (OIDC wired up)
#  3. The public /api/health endpoint answers 200 (proves the REST stack is up)
#
# Full login + SPA browse flow is interactive (OIDC authorization code + PKCE
# requires a browser); this script just gates the deployment shape.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
pass=0; fail=0
ok()     { echo -e "  ${GREEN}PASS${NC}  $*"; pass=$((pass+1)); }
fail_t() { echo -e "  ${RED}FAIL${NC}  $*"; fail=$((fail+1)); }

echo -e "${CYAN}══ Checking kafka-ui Deployment ══${NC}"

if ! kubectl --context "${CTX}" -n "${NS}" get deploy kafka-ui >/dev/null 2>&1; then
    echo "kafka-ui Deployment missing. Run: make kafka-ui-setup"
    exit 1
fi

if kubectl --context "${CTX}" -n "${NS}" wait --for=condition=Available \
        deploy/kafka-ui --timeout=60s >/dev/null 2>&1; then
    ok "Deployment Available"
else
    fail_t "Deployment not Available within 60s"
fi

POD=$(kubectl --context "${CTX}" -n "${NS}" get pods -l app=kafka-ui \
        --no-headers -o custom-columns=:.metadata.name | head -1)
[ -n "${POD}" ] || { echo "No kafka-ui pod found"; exit 1; }
echo "Using pod: ${POD}"

echo ""
echo -e "${CYAN}══ Pod readiness ══${NC}"
# Kubelet's HTTP probe to /q/health/ready already gates Ready=True, so reading
# the pod condition is the most portable check (no wget/curl assumed inside the
# UBI9 minimal image).
READY=$(kubectl --context "${CTX}" -n "${NS}" get pod "${POD}" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')
if [ "${READY}" = "True" ]; then
    ok "pod Ready=True (kubelet's /q/health/ready probe passed)"
else
    fail_t "pod Ready=${READY:-<unknown>}"
fi

echo ""
echo -e "${CYAN}══ OIDC redirect ══${NC}"
# Probe from inside the cluster via a one-shot curl Pod — UBI9-minimal lacks
# wget/curl, so we use a busybox sidecar via kubectl debug.
LOC=$(kubectl --context "${CTX}" -n "${NS}" run --rm -i --restart=Never \
        --image=curlimages/curl:8.10.1 ui-smoke-$$ -- \
        curl -s -o /dev/null -w '%{redirect_url}' http://kafka-ui.${NS}.svc.cluster.local:8080/ 2>/dev/null)
if [[ "${LOC}" == *"/realms/demo/protocol/openid-connect/auth"* ]]; then
    ok "GET / redirects to Keycloak (OIDC wired)"
else
    fail_t "Expected 302 → Keycloak authorize; got: ${LOC:-<none>}"
fi

echo ""
echo -e "${CYAN}══ Public /api/health ══${NC}"
HC=$(kubectl --context "${CTX}" -n "${NS}" run --rm -i --restart=Never \
        --image=curlimages/curl:8.10.1 ui-health-$$ -- \
        curl -s -o /dev/null -w '%{http_code}' http://kafka-ui.${NS}.svc.cluster.local:8080/api/health 2>/dev/null)
if [ "${HC}" = "200" ]; then
    ok "GET /api/health → 200 (REST stack up)"
else
    fail_t "Expected 200 from /api/health; got: ${HC:-<none>}"
fi

echo ""
if [ ${fail} -gt 0 ]; then
    echo -e "${RED}${fail} failure(s); ${pass} passed${NC}"
    exit 1
fi
echo -e "${GREEN}All ${pass} checks passed${NC}"
echo ""
echo "Interactive manual test:"
echo "  kubectl --context ${CTX} -n ${NS} port-forward svc/kafka-ui 8080:8080"
echo "  Open http://localhost:8080/ in a browser → Keycloak login as alice/alice"
