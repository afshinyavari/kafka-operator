#!/usr/bin/env bash
# update-hosts.sh — resync /etc/hosts with the kind cluster's live MetalLB IPs.
#
# Why this exists:
#   The demo reaches Keycloak and the Kafka UI from the host browser via their
#   Submariner names (e.g. kafka-ui.kafka.svc.clusterset.local). Those names only
#   resolve in-cluster, so /etc/hosts bridges them to the MetalLB LoadBalancer IPs.
#   MetalLB hands out IPs from its pool in service-creation order, which changes on
#   every `make teardown` + `mcs-setup` — so static /etc/hosts entries go stale and
#   the OIDC login redirect (UI -> Keycloak) breaks. Run this after each setup.
#
# Usage:
#   kind/update-hosts.sh             # rewrite /etc/hosts (prompts once for sudo)
#   kind/update-hosts.sh --dry-run   # print the result, change nothing, no sudo
set -euo pipefail

CTX="${CTX:-kind-kafka-a}"
NS="${NS:-kafka}"
HOSTS_FILE="${HOSTS_FILE:-/etc/hosts}"
DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

BEGIN_MARK="# >>> kind kafka-operator (managed by kind/update-hosts.sh) >>>"
END_MARK="# <<< kind kafka-operator <<<"

# host name -> LoadBalancer Service that fronts it
declare -A SVC=(
  ["keycloak.kafka.svc.clusterset.local"]="keycloak-lb"
  ["kafka-ui.kafka.svc.clusterset.local"]="kafka-ui"
)

# ── Resolve live LB IPs into a managed block ────────────────────────────────
block="${BEGIN_MARK}"$'\n'
missing=0
for host in "${!SVC[@]}"; do
  svc="${SVC[$host]}"
  ip="$(kubectl --context "${CTX}" -n "${NS}" get svc "${svc}" \
        -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  if [[ -z "${ip}" ]]; then
    echo "WARN: Service ${NS}/${svc} has no LoadBalancer IP yet — skipping ${host}" >&2
    missing=1
    continue
  fi
  printf '  %-15s -> %s\n' "${ip}" "${host}" >&2
  block+="${ip}  ${host}"$'\n'
done
block+="${END_MARK}"

# ── Strip the old managed block + any stray lines for these hostnames ────────
names_re=""
for host in "${!SVC[@]}"; do
  esc="${host//./\\.}"
  names_re+="${names_re:+|}${esc}"
done

new_content="$(awk -v b="${BEGIN_MARK}" -v e="${END_MARK}" -v names="${names_re}" '
  $0 == b { skip=1; next }
  $0 == e { skip=0; next }
  skip    { next }
  $0 ~ ("[[:space:]](" names ")([[:space:]]|$)") { next }
  { print }
' "${HOSTS_FILE}")"
new_content+=$'\n'"${block}"$'\n'

# ── Apply ───────────────────────────────────────────────────────────────────
if [[ "${DRY_RUN}" == "1" ]]; then
  echo "── /etc/hosts would become (dry-run): ──" >&2
  printf '%s' "${new_content}"
  exit 0
fi

tmp="$(mktemp)"
printf '%s' "${new_content}" > "${tmp}"
echo "Updating ${HOSTS_FILE} (sudo — backup at ${HOSTS_FILE}.bak)…" >&2
sudo cp "${HOSTS_FILE}" "${HOSTS_FILE}.bak"
sudo cp "${tmp}" "${HOSTS_FILE}"
rm -f "${tmp}"
echo "Done — /etc/hosts cluster entries resynced." >&2
[[ "${missing}" == "1" ]] && echo "NOTE: some services had no IP; re-run once they're up." >&2
exit 0
