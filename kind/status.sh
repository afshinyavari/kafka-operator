#!/bin/bash
# Shows operator, pod, and KafkaCluster status across all 3 clusters.
set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CLUSTERS=(kafka-a kafka-b kafka-c)
CLUSTER_IDS=(A B C)
NAMESPACE=kafka

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
header() { echo -e "\n${CYAN}══ $* ══${NC}"; }
label()  { printf "%-20s" "$1"; }

for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  id="${CLUSTER_IDS[$i]}"
  ctx="kind-${cluster}"

  header "Cluster ${id}  (${cluster})"

  echo ""
  label "Operator pod:"
  kubectl --context "${ctx}" get pods -n "${NAMESPACE}" -l app=kafka-operator \
    --no-headers -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,READY:.status.conditions[?(@.type=="Ready")].status' 2>/dev/null \
    || echo "  (none)"

  echo ""
  label "KafkaCluster:"
  kubectl --context "${ctx}" get kafkacluster -n "${NAMESPACE}" \
    --no-headers -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,MSG:.status.message' 2>/dev/null \
    || echo "  (not found)"

  echo ""
  label "KafkaNodePools:"
  kubectl --context "${ctx}" get kafkanodepool -n "${NAMESPACE}" \
    --no-headers -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,READY:.status.readyReplicas,DESIRED:.status.desiredReplicas' 2>/dev/null \
    || echo "  (none)"

  echo ""
  label "Pods:"
  kubectl --context "${ctx}" get pods -n "${NAMESPACE}" \
    -l "kafka.yavari.afshin.se/cluster=my-kafka" \
    --no-headers -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,NODE_ID:.metadata.labels.kafka\.yavari\.afshin\.se/node-id' 2>/dev/null \
    || echo "  (none)"

  echo ""
  label "KafkaPodSets:"
  kubectl --context "${ctx}" get kafkapodset -n "${NAMESPACE}" \
    --no-headers -o custom-columns='NAME:.metadata.name,REPLICAS:.status.replicas,READY:.status.readyReplicas' 2>/dev/null \
    || echo "  (none)"
done

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo "Tip: run 'kind/quorum.sh' to verify the KRaft controller quorum"
