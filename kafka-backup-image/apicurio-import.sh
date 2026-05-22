#!/usr/bin/env bash
# Imports a previously exported Apicurio artifact zip BEFORE topic records are
# restored, so restored records remain decodable. Runs as an init container in the
# KafkaRestore Job pod (see BackupWorkloadBuilder) — ordered ahead of the restore.
#
# Required env:
#   APICURIO_URL   base URL of the Apicurio registry (or its RBAC proxy)
#   SCHEMA_SRC     where the export zip lives — an absolute path (filesystem/PVC) or
#                  a "<bucket>/<prefix>" object-storage location
# Optional env: see apicurio-export.sh (APICURIO_IMPORT_PATH defaults to
#   /apis/registry/v2/admin/import; OAUTH_* and MC_ALIAS_* as in the export script).
#
# A missing export is treated as a no-op (exit 0) so restores of schema-less
# backups still proceed.
set -euo pipefail

: "${APICURIO_URL:?APICURIO_URL is required}"
: "${SCHEMA_SRC:?SCHEMA_SRC is required}"
IMPORT_PATH="${APICURIO_IMPORT_PATH:-/apis/registry/v2/admin/import}"
ZIP="/tmp/apicurio-schemas-import.zip"

if [[ "${SCHEMA_SRC}" == /* ]]; then
  latest=$(ls -1t "${SCHEMA_SRC}"/apicurio-schemas-*.zip 2>/dev/null | head -n1 || true)
  if [[ -z "${latest}" ]]; then
    echo "No schema export found in ${SCHEMA_SRC} — skipping import"; exit 0
  fi
  cp "${latest}" "${ZIP}"
else
  mc alias set backup "${MC_ALIAS_URL:?}" "${MC_ALIAS_ACCESS_KEY:?}" "${MC_ALIAS_SECRET_KEY:?}"
  latest=$(mc --quiet ls "backup/${SCHEMA_SRC}/" \
    | awk '{print $NF}' | grep -E '^apicurio-schemas-.*\.zip$' | sort | tail -n1 || true)
  if [[ -z "${latest}" ]]; then
    echo "No schema export found at ${SCHEMA_SRC} — skipping import"; exit 0
  fi
  mc cp "backup/${SCHEMA_SRC}/${latest}" "${ZIP}"
fi

auth=()
if [[ -n "${OAUTH_TOKEN_URL:-}" ]]; then
  token=$(curl -sSf -X POST "${OAUTH_TOKEN_URL}" \
    -d grant_type=client_credentials \
    -d "client_id=${OAUTH_CLIENT_ID:-}" \
    -d "client_secret=${OAUTH_CLIENT_SECRET:-}" | jq -r '.access_token')
  auth=(-H "Authorization: Bearer ${token}")
fi

echo "Importing Apicurio artifacts to ${APICURIO_URL}${IMPORT_PATH}"
curl -sSf "${auth[@]}" -X POST \
  -H "Content-Type: application/zip" \
  --data-binary "@${ZIP}" \
  "${APICURIO_URL}${IMPORT_PATH}"
echo "Apicurio import complete"
