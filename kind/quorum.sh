#!/bin/bash
# Verifies KRaft controller quorum health by exec-ing into the controller pod on each cluster.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CLUSTERS=(kafka-a kafka-b kafka-c)
CLUSTER_IDS=(A B C)
NAMESPACE=kafka

CYAN='\033[0;36m'; GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
header() { echo -e "\n${CYAN}══ $* ══${NC}"; }

for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  id="${CLUSTER_IDS[$i]}"
  ctx="kind-${cluster}"

  header "KRaft quorum from Cluster ${id}  (${cluster})"

  suffix="${id,,}"
  CONTROLLER_POOL="controllers-${suffix}"
  BROKER_POOL="brokers-${suffix}"

  # Find the controller pod on this cluster
  POD=$(kubectl --context "${ctx}" get pods -n "${NAMESPACE}" \
    -l "kafka.yavari.afshin.se/node-pool=${CONTROLLER_POOL},kafka.yavari.afshin.se/cluster=my-kafka" \
    --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)

  if [ -z "${POD}" ]; then
    echo -e "${RED}No controller pod found on ${cluster}${NC}"
    continue
  fi

  echo "Controller pod: ${POD}"
  echo ""

  # Prefer a broker pod for quorum check (--bootstrap-controller requires newer MetadataVersion)
  BROKER_POD=$(kubectl --context "${ctx}" get pods -n "${NAMESPACE}" \
    -l "kafka.yavari.afshin.se/node-pool=${BROKER_POOL},kafka.yavari.afshin.se/cluster=my-kafka" \
    --no-headers -o custom-columns='NAME:.metadata.name' 2>/dev/null | head -1)

  if [ -n "${BROKER_POD}" ]; then
    echo "Using broker pod for quorum check: ${BROKER_POD}"
    kubectl --context "${ctx}" exec -n "${NAMESPACE}" "${BROKER_POD}" -- \
      /opt/kafka/bin/kafka-metadata-quorum.sh \
        --bootstrap-server "localhost:9092" \
        describe --status 2>/dev/null && echo -e "${GREEN}✓ Quorum healthy${NC}" \
      || echo -e "${RED}✗ Quorum check failed (cluster may still be bootstrapping)${NC}"
  else
    # Fall back to controller-direct check
    echo "No broker pod found — falling back to controller-direct check"
    kubectl --context "${ctx}" exec -n "${NAMESPACE}" "${POD}" -- \
      /opt/kafka/bin/kafka-metadata-quorum.sh \
        --bootstrap-controller "localhost:9093" \
        describe --status 2>/dev/null && echo -e "${GREEN}✓ Quorum healthy${NC}" \
      || echo -e "${RED}✗ Quorum check failed (controller may still be starting)${NC}"
  fi
done
