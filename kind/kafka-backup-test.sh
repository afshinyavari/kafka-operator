#!/bin/bash
# kafka-backup smoke test (joins SMOKE_E2E):
#  1. Build + load the kafka-backup image into all kind clusters.
#  2. Apply a KafkaBackup CR (PVC storage) — assert the operator builds a CronJob
#     + ConfigMap and reports status.phase=SCHEDULED.
#  3. Apply a KafkaRestore CR without spec.confirm — assert the reconciler refuses it.
#  4. Apply a KafkaBackupValidation CR — assert the operator creates a one-shot Job.
#  5. Delete all three and verify cascade cleanup.
#
# This verifies CRD + reconciler wiring. A full backup -> restore -> validate round
# trip against object storage is the user-run extended test.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CTX="kind-kafka-a"
NS="kafka"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "  ${GREEN}PASS${NC}  $*"; }
fail() { echo -e "  ${RED}FAIL${NC}  $*"; exit 1; }

echo "=== kafka-backup smoke ==="

# Build + load the kafka-backup image so the workloads can pull it locally.
make kafka-backup-image >/dev/null
for cluster in kafka-a kafka-b kafka-c; do
  kind load docker-image kafka-backup:dev --name "${cluster}" >/dev/null
done
ok "kafka-backup:dev built and loaded into all kind clusters"

# ── KafkaBackup ──────────────────────────────────────────────────────────────
kubectl --context "${CTX}" apply -f manifests/kafka-backup-cr.yaml >/dev/null
ok "KafkaBackup CR applied"

PHASE=""
for i in $(seq 1 60); do
  PHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkabackup kafka-backup-smoke \
            -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${PHASE}" == "SCHEDULED" || "${PHASE}" == "FAILED" ]]; then
    break
  fi
  sleep 2
done
[[ "${PHASE}" == "SCHEDULED" ]] || fail "expected KafkaBackup phase SCHEDULED, got '${PHASE}'"
ok "KafkaBackup status.phase=SCHEDULED"

kubectl --context "${CTX}" -n "${NS}" get cronjob kafka-backup-smoke >/dev/null 2>&1 \
  || fail "CronJob kafka-backup-smoke not created"
ok "CronJob kafka-backup-smoke present"

kubectl --context "${CTX}" -n "${NS}" get configmap kafka-backup-smoke >/dev/null 2>&1 \
  || fail "ConfigMap kafka-backup-smoke not created"
ok "ConfigMap kafka-backup-smoke present"

# ── KafkaRestore confirm guard ───────────────────────────────────────────────
kubectl --context "${CTX}" apply -f - >/dev/null <<'EOF'
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRestore
metadata:
  name: kafka-restore-smoke
  namespace: kafka
spec:
  image: kafka-backup:dev
  targetClusterRef:
    name: my-kafka
  placement:
    clusterId: A
  confirm: false
  source:
    kafkaBackupRef: kafka-backup-smoke
EOF

RPHASE=""
for i in $(seq 1 30); do
  RPHASE=$(kubectl --context "${CTX}" -n "${NS}" get kafkarestore kafka-restore-smoke \
             -o jsonpath='{.status.phase}' 2>/dev/null || true)
  if [[ "${RPHASE}" == "FAILED" || "${RPHASE}" == "RUNNING" || "${RPHASE}" == "SUCCEEDED" ]]; then
    break
  fi
  sleep 2
done
[[ "${RPHASE}" == "FAILED" ]] \
  || fail "expected KafkaRestore phase FAILED (confirm guard), got '${RPHASE}'"
ok "KafkaRestore without spec.confirm is rejected (status.phase=FAILED)"

# ── KafkaBackupValidation ────────────────────────────────────────────────────
kubectl --context "${CTX}" apply -f manifests/kafka-backup-validation-cr.yaml >/dev/null
for i in $(seq 1 60); do
  kubectl --context "${CTX}" -n "${NS}" get job kafka-backup-validation-smoke >/dev/null 2>&1 \
    && break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get job kafka-backup-validation-smoke >/dev/null 2>&1 \
  || fail "validation Job kafka-backup-validation-smoke not created"
ok "KafkaBackupValidation one-shot Job created"

# ── Cleanup + cascade ────────────────────────────────────────────────────────
kubectl --context "${CTX}" -n "${NS}" delete kafkarestore kafka-restore-smoke >/dev/null
kubectl --context "${CTX}" -n "${NS}" delete kafkabackupvalidation \
  kafka-backup-validation-smoke >/dev/null
kubectl --context "${CTX}" delete -f manifests/kafka-backup-cr.yaml >/dev/null

for i in $(seq 1 30); do
  kubectl --context "${CTX}" -n "${NS}" get cronjob kafka-backup-smoke >/dev/null 2>&1 || break
  sleep 2
done
kubectl --context "${CTX}" -n "${NS}" get cronjob kafka-backup-smoke >/dev/null 2>&1 \
  && fail "CronJob not cascade-deleted with the KafkaBackup CR"
ok "CRs deleted and child resources cascade-cleaned"

echo ""
echo -e "${GREEN}✓ kafka-backup smoke passed${NC}"
