#!/bin/bash
# MirrorMaker2 data + Apicurio schema mirror e2e (joins EXTENDED_E2E).
#
# Topology:
#   source — single-node Kafka + standalone Apicurio in the `kafka-src` namespace
#            (not operator-managed; MM2 reaches it as an `external` endpoint).
#   target — the operator-managed `my-kafka` cluster + its RBAC-proxied Apicurio.
#
# Verifies, end to end:
#   1. JSON records produced to the source topic are mirrored verbatim to
#      `source.mm2-orders` on the target (DefaultReplicationPolicy renames topics with
#      the source-cluster alias).
#   2. the schema-sync SMT mirrors the Apicurio schema into the managed registry —
#      authenticating to its apicurio-rbac-proxy with OAuth2 client-credentials — and
#      rewrites each record's envelope globalId to the target registry's id.
#
# Binary Apicurio-envelope records can't survive kafka-console-producer's charset
# handling, so produce/consume go through the in-cluster Mm2MirrorProbe (shipped in
# the mm2:dev image) run via kafka-run-class.sh.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"
cd "$(dirname "$0")"

CTX="kind-kafka-a"
NS="kafka"
SRC_NS="kafka-src"
CR="mm2-mirror"
SRC_TOPIC="mm2-orders"
TGT_TOPIC="source.mm2-orders"          # DefaultReplicationPolicy: <sourceAlias>.<topic>
RUN_ID="$$"
RECORDS=5
KC_TOKEN_URL="http://keycloak.${NS}.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/token"
PROBE="se.afshin.yavari.kafka.smt.Mm2MirrorProbe"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
diag() {
  echo "── MM2 status ──"
  kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" -o jsonpath='{.status}' 2>/dev/null \
    | python3 -m json.tool 2>/dev/null || true
  echo "── MM2 worker log (tail 200) ──"
  kubectl --context "${CTX}" -n "${NS}" logs deploy/"${CR}" --tail=200 2>/dev/null || true
}
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

echo "=== MM2 mirror + schema-sync e2e (run ${RUN_ID}) ==="

cleanup() {
  if [[ "${MM2_TEST_KEEP:-0}" == "1" ]]; then
    echo "── cleanup skipped (MM2_TEST_KEEP=1) — resources left for inspection ──"
    return
  fi
  echo "── cleanup ──"
  kubectl --context "${CTX}" delete -f manifests/mm2-mirror-cr.yaml --ignore-not-found >/dev/null 2>&1 || true
  kubectl --context "${CTX}" -n "${NS}" delete secret mm2-schema-registry-oauth \
    --ignore-not-found >/dev/null 2>&1 || true
  kubectl --context "${CTX}" delete namespace "${SRC_NS}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ── 0. Build + load the MM2 image (carries the schema-sync-smt JAR + Mm2MirrorProbe) ──
make mm2-image >/dev/null
for c in kafka-a kafka-b kafka-c; do
  kind load docker-image mm2:dev --name "${c}" >/dev/null
done
ok "mm2:dev built and loaded into all kind clusters"

# ── 1. Source cluster: single-node Kafka + standalone Apicurio in kafka-src ──────────
kubectl --context "${CTX}" create namespace "${SRC_NS}" --dry-run=client -o yaml \
  | kubectl --context "${CTX}" apply -f - >/dev/null
kubectl --context "${CTX}" apply -f manifests/mm2-src-kafka.yaml >/dev/null
kubectl --context "${CTX}" apply -f manifests/mm2-src-apicurio.yaml >/dev/null
# src-kafka is long-lived across MM2_TEST_KEEP re-runs. A plain `apply` of an
# unchanged StatefulSet won't restart it onto the mm2:dev image just rebuilt in
# step 0 — leaving a stale in-pod Mm2MirrorProbe. Force a roll so the probe is current.
kubectl --context "${CTX}" -n "${SRC_NS}" rollout restart statefulset/src-kafka >/dev/null 2>&1 || true
kubectl --context "${CTX}" -n "${SRC_NS}" rollout status statefulset/src-kafka --timeout=180s >/dev/null
ok "source Kafka ready"
kubectl --context "${CTX}" -n "${SRC_NS}" rollout status deployment/src-apicurio --timeout=300s >/dev/null
ok "source Apicurio ready"

# ── 2. Source topic ─────────────────────────────────────────────────────────────────
kubectl --context "${CTX}" -n "${SRC_NS}" exec src-kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "${SRC_TOPIC}" --partitions 1 --replication-factor 1 >/dev/null
ok "source topic ${SRC_TOPIC} created"

# ── 3. Register a JSON schema in the source Apicurio (v2 API) ────────────────────────
SCHEMA="{\"\$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"order-${RUN_ID}\",\"type\":\"object\",\"required\":[\"id\"],\"properties\":{\"id\":{\"type\":\"string\"},\"amount\":{\"type\":\"integer\"}}}"
REG_RESP=$(kubectl --context "${CTX}" -n "${SRC_NS}" exec src-kafka-0 -- \
  curl -sf -X POST \
  'http://src-apicurio.kafka-src.svc.cluster.local:8080/apis/registry/v2/groups/default/artifacts?ifExists=RETURN_OR_UPDATE' \
  -H 'X-Registry-ArtifactId: mm2-orders-value' \
  -H 'X-Registry-ArtifactType: JSON' \
  -H 'Content-Type: application/json' -d "${SCHEMA}") \
  || fail "schema registration on source Apicurio failed"
SRC_GID=$(echo "${REG_RESP}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["globalId"])') \
  || fail "could not parse source globalId from: ${REG_RESP}"
ok "source schema registered (globalId=${SRC_GID})"

# ── 4. Produce schema-encoded records to the source topic ───────────────────────────
kubectl --context "${CTX}" -n "${SRC_NS}" exec src-kafka-0 -- \
  /opt/kafka/bin/kafka-run-class.sh "${PROBE}" produce \
  --bootstrap localhost:9092 --topic "${SRC_TOPIC}" \
  --global-id "${SRC_GID}" --count "${RECORDS}" --prefix "run-${RUN_ID}-" --json >/dev/null \
  || fail "producing source records failed"
ok "produced ${RECORDS} JSON records (Apicurio-enveloped) to the source"

# ── 5. OAuth Secret for the SMT + the MirrorMaker2 CR ───────────────────────────────
kubectl --context "${CTX}" -n "${NS}" create secret generic mm2-schema-registry-oauth \
  --from-literal=token-url="${KC_TOKEN_URL}" \
  --from-literal=client-id="mm2-schema-sync" \
  --from-literal=client-secret="mm2-schema-sync-secret" \
  --dry-run=client -o yaml | kubectl --context "${CTX}" apply -f - >/dev/null
kubectl --context "${CTX}" apply -f manifests/mm2-mirror-cr.yaml >/dev/null
ok "OAuth Secret + MirrorMaker2 CR applied"

# ── 6. Wait for the MM2 reconcile to reach READY ────────────────────────────────────
PHASE=""
for _ in $(seq 1 75); do
  PHASE=$(kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" \
            -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [[ "${PHASE}" == "READY" ]] && break
  if [[ "${PHASE}" == "FAILED" ]]; then
    kubectl --context "${CTX}" -n "${NS}" get mm2 "${CR}" -o jsonpath='{.status.message}'; echo
    fail "MM2 reconcile FAILED"
  fi
  sleep 4
done
if [[ "${PHASE}" != "READY" ]]; then
  kubectl --context "${CTX}" -n "${NS}" describe mm2 "${CR}" | tail -20
  kubectl --context "${CTX}" -n "${NS}" logs deploy/"${CR}" --tail=40 2>/dev/null || true
  fail "MM2 not READY after ~5m (phase=${PHASE})"
fi
ok "MM2 status.phase=READY"

for t in mm2-configs.mirror mm2-offsets.mirror mm2-status.mirror; do
  kubectl --context "${CTX}" -n "${NS}" get kafkatopic "${t}" >/dev/null 2>&1 \
    || fail "internal KafkaTopic ${t} missing"
done
ok "3 internal KafkaTopic CRs present"

# ── 7. Consume the mirrored topic from the target (through the proxy, mTLS) ──────────
echo "── waiting for mirrored data on ${TGT_TOPIC} ──"
CONSUME_OUT=""; MATCHED=0
for i in $(seq 1 12); do
  CONSUME_OUT=$(kubectl --context "${CTX}" -n "${NS}" exec deploy/"${CR}" -- \
    /opt/kafka/bin/kafka-run-class.sh "${PROBE}" consume \
    --bootstrap kafka-proxy.kafka.svc.cluster.local:9094 \
    --topic "${TGT_TOPIC}" --group "mm2-probe-${RUN_ID}-${i}" \
    --timeout-ms 30000 --expect "${RECORDS}" \
    --ssl --keystore /etc/mm2/pkcs12/target/keystore.p12 \
    --truststore /etc/mm2/pkcs12/target/truststore.p12 \
    --store-password changeit 2>/dev/null || true)
  MATCHED=$(echo "${CONSUME_OUT}" | grep -c "run-${RUN_ID}-" || true)
  [[ "${MATCHED}" -ge "${RECORDS}" ]] && break
  sleep 6
done
if [[ "${MATCHED}" -lt "${RECORDS}" ]]; then
  diag
  fail "expected ≥${RECORDS} mirrored records on ${TGT_TOPIC}, got ${MATCHED}"
fi
ok "${MATCHED} records mirrored to ${TGT_TOPIC}"

# Every mirrored record must still carry an Apicurio envelope, with a rewritten globalId.
MIRRORED_LINE=$(echo "${CONSUME_OUT}" | grep "run-${RUN_ID}-" | head -1)
TGT_GID=$(echo "${MIRRORED_LINE}" | sed -n 's/.*gid=\(-\{0,1\}[0-9]\+\).*/\1/p')
[[ -n "${TGT_GID}" && "${TGT_GID}" != "-1" ]] \
  || fail "mirrored records carry no valid Apicurio envelope (gid=${TGT_GID})"
ok "mirrored records carry a rewritten envelope (target globalId=${TGT_GID})"

# The SMT rewrites only the envelope, never the payload — the mirrored value must
# survive as the original, valid JSON message conforming to the {id, amount} schema.
TGT_PAYLOAD=$(echo "${MIRRORED_LINE}" | sed -n 's/.*payload=//p')
echo "${TGT_PAYLOAD}" | python3 -c 'import sys,json; o=json.load(sys.stdin); assert isinstance(o.get("id"),str) and isinstance(o.get("amount"),int), o' \
  || fail "mirrored payload is not valid {id, amount} JSON: ${TGT_PAYLOAD}"
ok "mirrored payload is intact JSON conforming to the source schema (${TGT_PAYLOAD})"

# ── 8. The rewritten globalId must resolve to our schema on the MANAGED registry ─────
BROKER_POD=$(kubectl --context "${CTX}" -n "${NS}" get pods \
  -l "kafka.yavari.afshin.se/node-pool=brokers-a,kafka.yavari.afshin.se/cluster=my-kafka" \
  --no-headers -o custom-columns='NAME:.metadata.name' | head -1)
[[ -n "${BROKER_POD}" ]] || fail "no broker pod on ${CTX}"

JWT=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  curl -sf -X POST '${KC_TOKEN_URL}' \
    -d 'grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=schemadmin&password=schemadmin' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")'")
[[ -n "${JWT}" ]] || fail "could not obtain a schemadmin JWT"

if ! TGT_SCHEMA=$(kubectl --context "${CTX}" -n "${NS}" exec "${BROKER_POD}" -- bash -c "
  curl -sf -H 'Authorization: Bearer ${JWT}' \
    'http://apicurio-rbac-proxy.kafka.svc.cluster.local:8082/apis/registry/v2/ids/globalIds/${TGT_GID}'"); then
  diag
  fail "globalId ${TGT_GID} does not resolve on the managed registry — schema-sync did not run"
fi
if ! echo "${TGT_SCHEMA}" | grep -q "order-${RUN_ID}"; then
  diag
  fail "managed registry globalId ${TGT_GID} has unexpected content: ${TGT_SCHEMA}"
fi
ok "schema mirrored into the managed registry; mirrored record envelopes resolve there"

echo ""
echo -e "${GREEN}✓ MM2 mirror + schema-sync e2e passed${NC}"
