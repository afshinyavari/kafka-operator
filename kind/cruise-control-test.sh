#!/bin/bash
# cruise-control smoke test (joins SMOKE_E2E):
#  1. Build + load the Cruise Control image into all kind clusters.
#  2. Enable spec.cruiseControl on KafkaCluster my-kafka — assert the operator deploys
#     Cruise Control and reports status.cruiseControl.phase=READY.
#  3. Apply a KafkaRebalance CR — assert the reconciler drives Cruise Control and the CR
#     reaches PENDING_PROPOSAL / PROPOSAL_READY.
#  4. Assert the rebalance does NOT auto-execute without the approve annotation.
#  5. Delete the KafkaRebalance CR.
#
# This verifies CRD + reconciler + image wiring. A full proposal -> approve -> execute
# round trip (Cruise Control needs minutes of metric windows) is the user-run extended
# test. Cruise Control is left enabled on my-kafka so re-runs are idempotent.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() {
  echo -e "  ${RED}FAIL${NC}  $*"
  kubectl --context "${CTX}" -n "${NS}" get kafkacluster my-kafka \
    -o jsonpath='{.status.cruiseControl}' 2>/dev/null || true
  echo ""
  kubectl --context "${CTX}" -n "${NS}" get kafkarebalance cc-rebalance-smoke \
    -o jsonpath='{.status}' 2>/dev/null || true
  echo ""
  exit 1
}

echo "=== cruise-control smoke ==="

# Build + load the Cruise Control image so the Deployment can pull it locally.
make reload-cruise-control-image >/dev/null
ok "cruise-control image built and loaded into all kind clusters"

# Idempotent re-run: drop any leftover KafkaRebalance from a prior run.
kubectl --context "${CTX}" -n "${NS}" delete kafkarebalance cc-rebalance-smoke \
  --ignore-not-found >/dev/null 2>&1 || true

# ── Enable Cruise Control ────────────────────────────────────────────────────
kubectl --context "${CTX}" -n "${NS}" patch kafkacluster my-kafka --type merge \
  -p '{"spec":{"cruiseControl":{}}}' >/dev/null
ok "spec.cruiseControl enabled on KafkaCluster my-kafka"

CCPHASE=""
for i in $(seq 1 90); do
  CCPHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkacluster my-kafka \
            -o jsonpath='{.status.cruiseControl.phase}' 2>/dev/null || true)
  if [[ "${CCPHASE}" == "READY" || "${CCPHASE}" == "FAILED" ]]; then
    break
  fi
  sleep 4
done
[[ "${CCPHASE}" == "READY" ]] || fail "expected status.cruiseControl.phase=READY, got '${CCPHASE}'"
ok "Cruise Control deployed (status.cruiseControl.phase=READY)"

kubectl --context "${CTX}" -n "${NS}" get deployment cruise-control >/dev/null 2>&1 \
  || fail "cruise-control Deployment not created"
kubectl --context "${CTX}" -n "${NS}" get svc cruise-control >/dev/null 2>&1 \
  || fail "cruise-control Service not created"
ok "cruise-control Deployment + Service present"

# ── KafkaRebalance proposal ──────────────────────────────────────────────────
kubectl --context "${CTX}" apply -f manifests/kafka-rebalance-cr.yaml >/dev/null
ok "KafkaRebalance CR applied"

RPHASE=""
for i in $(seq 1 60); do
  RPHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkarebalance cc-rebalance-smoke \
           -o jsonpath='{.status.phase}' 2>/dev/null || true)
  case "${RPHASE}" in
    PENDING_PROPOSAL|PROPOSAL_READY|NOT_READY|REBALANCING|READY) break ;;
  esac
  sleep 3
done
case "${RPHASE}" in
  PENDING_PROPOSAL|PROPOSAL_READY)
    ok "KafkaRebalance reached ${RPHASE} — reconciler drove Cruise Control" ;;
  *)
    fail "expected KafkaRebalance PENDING_PROPOSAL/PROPOSAL_READY, got '${RPHASE}'" ;;
esac

# The proposal must NOT execute without the approve annotation.
[[ "${RPHASE}" != "REBALANCING" && "${RPHASE}" != "READY" ]] \
  || fail "KafkaRebalance executed without the approve annotation"
ok "KafkaRebalance did not auto-execute without approval"

# ── Cleanup (Cruise Control stays enabled — re-runs are idempotent) ──────────
kubectl --context "${CTX}" -n "${NS}" delete kafkarebalance cc-rebalance-smoke >/dev/null
ok "KafkaRebalance CR deleted"

echo ""
echo -e "${GREEN}✓ cruise-control smoke passed${NC}"
