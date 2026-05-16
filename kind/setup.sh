#!/bin/bash
# Full Kind multi-cluster setup for the Kafka operator.
# Creates 3 Kind clusters (kafka-a, kafka-b, kafka-c), builds and loads the operator image,
# and deploys everything. All clusters share the Docker 'kind' network so controllers
# can reach each other via node IPs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"

IMAGE_NAME="kafka-operator:dev"
CLUSTERS=(kafka-a kafka-b kafka-c)
CLUSTER_IDS=(A B C)
CONTROLLER_NODEPORT=30093
NAMESPACE=kafka

# ── Colour helpers ──────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

# ── Ensure kubectl is available ──────────────────────────────────────────────
LOCAL_BIN="${HOME}/.local/bin"
mkdir -p "${LOCAL_BIN}"
# Prepend to PATH so we find a locally-installed kubectl even if not in system PATH
export PATH="${LOCAL_BIN}:${PATH}"

if ! command -v kubectl &>/dev/null; then
  warn "kubectl not found — downloading to ${LOCAL_BIN}/kubectl"
  K8S_VER=$(curl -sSL https://dl.k8s.io/release/stable.txt)
  curl -sSL "https://dl.k8s.io/release/${K8S_VER}/bin/linux/amd64/kubectl" \
    -o "${LOCAL_BIN}/kubectl"
  chmod +x "${LOCAL_BIN}/kubectl"
  ok "kubectl ${K8S_VER} installed at ${LOCAL_BIN}/kubectl"
else
  KUBECTL_PATH=$(command -v kubectl)
  ok "kubectl found at ${KUBECTL_PATH} ($(kubectl version --client --short 2>/dev/null || kubectl version --client 2>/dev/null | head -1))"
fi

# ── Step 1: Build the operator ───────────────────────────────────────────────
info "Building kafka-operator (Quarkus fast-jar)..."
cd "${OPERATOR_DIR}"
mvn package -DskipTests -q
ok "Build complete: target/quarkus-app/"

# ── Step 2: Build Docker image ───────────────────────────────────────────────
info "Building Docker image ${IMAGE_NAME}..."
docker build -t "${IMAGE_NAME}" "${OPERATOR_DIR}" -q
ok "Image ${IMAGE_NAME} built"

# ── Step 3: Create Kind clusters (skip if already running) ───────────────────
for cluster in "${CLUSTERS[@]}"; do
  if kind get clusters 2>/dev/null | grep -q "^${cluster}$"; then
    warn "Cluster '${cluster}' already exists — skipping creation"
  else
    info "Creating Kind cluster '${cluster}'..."
    kind create cluster --name "${cluster}" --config "${SCRIPT_DIR}/cluster.yaml" --wait 60s
    ok "Cluster '${cluster}' ready"
  fi
done

# ── Step 3b: Label nodes with topology zone (used as Kafka broker.rack) ────
info "Labeling Kind nodes with topology.kubernetes.io/zone..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  zone="zone-${CLUSTER_IDS[$i],,}"
  kubectl --context "kind-${cluster}" label node "${cluster}-control-plane" \
    topology.kubernetes.io/zone="${zone}" --overwrite
  ok "  ${cluster}: zone=${zone}"
done

# ── Step 4: Load image into each cluster ────────────────────────────────────
info "Loading ${IMAGE_NAME} into all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  kind load docker-image "${IMAGE_NAME}" --name "${cluster}"
done
ok "Image loaded into all clusters"

# ── Step 5: Get node IPs (Kind nodes share the Docker 'kind' network) ────────
info "Resolving Kind node IPs..."
NODE_A=$(docker inspect kafka-a-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
NODE_B=$(docker inspect kafka-b-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
NODE_C=$(docker inspect kafka-c-control-plane --format '{{.NetworkSettings.Networks.kind.IPAddress}}')

info "  Cluster A node IP: ${NODE_A}"
info "  Cluster B node IP: ${NODE_B}"
info "  Cluster C node IP: ${NODE_C}"

export CTRL_A_ADDR="${NODE_A}:${CONTROLLER_NODEPORT}"
export CTRL_B_ADDR="${NODE_B}:${CONTROLLER_NODEPORT}"
export CTRL_C_ADDR="${NODE_C}:${CONTROLLER_NODEPORT}"
ok "Controller addresses: A=${CTRL_A_ADDR}  B=${CTRL_B_ADDR}  C=${CTRL_C_ADDR}"

# ── Step 6: Apply CRDs to all clusters ──────────────────────────────────────
info "Applying CRDs to all clusters..."
CRD_DIR="${OPERATOR_DIR}/target/classes/META-INF/fabric8"
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkaclusters.kafka.yavari.afshin.se-v1.yml" --server-side
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkanodepools.kafka.yavari.afshin.se-v1.yml" --server-side
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkapodsets.kafka.yavari.afshin.se-v1.yml" --server-side
done
ok "CRDs applied"

# ── Step 7: Create namespace + RBAC on each cluster ─────────────────────────
info "Creating namespace '${NAMESPACE}' and RBAC on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/namespace.yaml" --server-side
  kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/rbac.yaml" --server-side
done
ok "Namespace and RBAC ready"

# ── Step 8: Deploy the operator (one per cluster, different KAFKA_CLUSTER_ID) ─
info "Deploying kafka-operator to each cluster..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  ctx="kind-${cluster}"
  info "  Deploying to ${cluster} with KAFKA_CLUSTER_ID=${cluster_id}..."
  KAFKA_CLUSTER_ID="${cluster_id}" KAFKA_NETWORKING_MCS_ENABLED="false" \
    envsubst < "${MANIFESTS_DIR}/operator.yaml" | \
    kubectl --context "${ctx}" apply --server-side -f -
done
ok "Operator deployed to all clusters"

# ── Step 9: Apply KafkaCluster CR (same to all clusters) ────────────────────
info "Generating and applying KafkaCluster CR..."
RENDERED_CR=$(mktemp /tmp/kafka-cluster-XXXXXX.yaml)
trap 'rm -f "${RENDERED_CR}"' EXIT

envsubst < "${MANIFESTS_DIR}/kafka-cluster.yaml" > "${RENDERED_CR}"
cat "${RENDERED_CR}"

for cluster in "${CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" apply --server-side -f "${RENDERED_CR}"
done
ok "KafkaCluster CR applied"

# ── Step 10: Apply NodePort service for cross-cluster controller access ───────
info "Applying controller NodePort service..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  CLUSTER_SUFFIX="${cluster_id,,}" envsubst < "${MANIFESTS_DIR}/controller-nodeport.yaml" | \
    kubectl --context "kind-${cluster}" apply --server-side -f -
done
ok "Controller NodePort (30093) applied"

# ── Step 11: Apply KafkaNodePools (cluster-prefixed names per cluster) ────────
info "Applying KafkaNodePools to all clusters..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  CLUSTER_SUFFIX="${cluster_id,,}" envsubst < "${MANIFESTS_DIR}/kafka-node-pools.yaml.tpl" | \
    kubectl --context "kind-${cluster}" apply --server-side -f -
done
ok "KafkaNodePools applied"

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${GREEN}Setup complete!${NC} Run 'kind/status.sh' to watch progress."
echo ""
echo "Useful commands:"
echo "  make -C kind status        # check all clusters"
echo "  make -C kind logs-a        # operator logs on cluster A"
echo "  make -C kind quorum        # verify KRaft quorum"
echo "  make -C kind teardown      # destroy all clusters"
echo "────────────────────────────────────────────────────────────────────────"
