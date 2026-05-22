#!/usr/bin/env bash
# Exports every Apicurio Registry artifact as a zip and stores it next to the Kafka
# topic backup, so a later restore can recreate decodable records. Runs as a sidecar
# container in the KafkaBackup CronJob pod (see BackupWorkloadBuilder).
#
# Required env:
#   APICURIO_URL   base URL of the Apicurio registry (or its RBAC proxy)
#   SCHEMA_DEST    destination for the export zip — an absolute path (filesystem/PVC
#                  storage) or a "<bucket>/<prefix>" object-storage target
# Optional env:
#   APICURIO_EXPORT_PATH                    default /apis/registry/v2/admin/export
#   OAUTH_TOKEN_URL / OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET
#                                           OAuth2 client-credentials, sent as Bearer
#   MC_ALIAS_URL / MC_ALIAS_ACCESS_KEY / MC_ALIAS_SECRET_KEY
#                                           object-storage endpoint + creds for `mc`
set -euo pipefail

: "${APICURIO_URL:?APICURIO_URL is required}"
: "${SCHEMA_DEST:?SCHEMA_DEST is required}"
EXPORT_PATH="${APICURIO_EXPORT_PATH:-/apis/registry/v2/admin/export}"
ZIP="/tmp/apicurio-schemas-$(date -u +%Y%m%dT%H%M%SZ).zip"

auth=()
if [[ -n "${OAUTH_TOKEN_URL:-}" ]]; then
  echo "Acquiring OAuth2 token from ${OAUTH_TOKEN_URL}"
  token=$(curl -sSf -X POST "${OAUTH_TOKEN_URL}" \
    -d grant_type=client_credentials \
    -d "client_id=${OAUTH_CLIENT_ID:-}" \
    -d "client_secret=${OAUTH_CLIENT_SECRET:-}" | jq -r '.access_token')
  auth=(-H "Authorization: Bearer ${token}")
fi

echo "Exporting Apicurio artifacts from ${APICURIO_URL}${EXPORT_PATH}"
curl -sSf "${auth[@]}" "${APICURIO_URL}${EXPORT_PATH}" -o "${ZIP}"
echo "Export written to ${ZIP} ($(stat -c%s "${ZIP}") bytes)"

if [[ "${SCHEMA_DEST}" == /* ]]; then
  mkdir -p "${SCHEMA_DEST}"
  cp "${ZIP}" "${SCHEMA_DEST}/"
  echo "Schema export copied to ${SCHEMA_DEST}/"
else
  mc alias set backup "${MC_ALIAS_URL:?}" "${MC_ALIAS_ACCESS_KEY:?}" "${MC_ALIAS_SECRET_KEY:?}"
  mc cp "${ZIP}" "backup/${SCHEMA_DEST}/"
  echo "Schema export uploaded to ${SCHEMA_DEST}/"
fi
