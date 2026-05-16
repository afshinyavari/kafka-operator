#!/bin/bash
# Destroys all Kind clusters created by setup.sh.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CLUSTERS=(kafka-a kafka-b kafka-c)
RED='\033[0;31m'; NC='\033[0m'

echo -e "${RED}Deleting Kind clusters: ${CLUSTERS[*]}${NC}"
for cluster in "${CLUSTERS[@]}"; do
  if kind get clusters 2>/dev/null | grep -q "^${cluster}$"; then
    kind delete cluster --name "${cluster}"
    echo "  Deleted ${cluster}"
  else
    echo "  ${cluster} not found — skipping"
  fi
done
echo "Done."
