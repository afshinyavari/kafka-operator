#!/bin/bash
# MCS setup: 3 Kind clusters with Flannel CNI + Submariner, kafka-operator with MCS enabled.
# Clusters communicate cross-cluster via *.svc.clusterset.local (Submariner Lighthouse).
# Run 'make -C kind teardown' first if clusters already exist.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"

IMAGE_NAME="kafka-operator:dev"
CLUSTERS=(kafka-a kafka-b kafka-c)
CLUSTER_IDS=(A B C)
CLUSTER_CONFIGS=(cluster-a.yaml cluster-b.yaml cluster-c.yaml)
POD_SUBNETS=(10.244.0.0/16 10.245.0.0/16 10.246.0.0/16)
NAMESPACE=kafka
FLANNEL_VERSION="v0.25.4"
CNI_PLUGINS_VERSION="v1.4.0"
SUBMARINER_VERSION="0.17.0"

# ── Colour helpers ──────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }

LOCAL_BIN="${HOME}/.local/bin"
mkdir -p "${LOCAL_BIN}"
export PATH="${LOCAL_BIN}:${PATH}"

# ── Ensure kubectl ──────────────────────────────────────────────────────────
if ! command -v kubectl &>/dev/null; then
  warn "kubectl not found — downloading to ${LOCAL_BIN}/kubectl"
  K8S_VER=$(curl -sSL https://dl.k8s.io/release/stable.txt)
  curl -sSL "https://dl.k8s.io/release/${K8S_VER}/bin/linux/amd64/kubectl" \
    -o "${LOCAL_BIN}/kubectl"
  chmod +x "${LOCAL_BIN}/kubectl"
  ok "kubectl ${K8S_VER} installed"
fi

# ── Ensure subctl ───────────────────────────────────────────────────────────
if ! command -v subctl &>/dev/null; then
  warn "subctl not found — installing via get.submariner.io into ${LOCAL_BIN}"
  curl -Ls https://get.submariner.io | DESTDIR="${LOCAL_BIN}" VERSION="${SUBMARINER_VERSION}" bash
  ok "subctl ${SUBMARINER_VERSION} installed"
else
  ok "subctl found: $(subctl version 2>/dev/null | head -1)"
fi

# ── Step 1 & 2: Build operator + Docker image (skip if image already exists) ─
if docker inspect "${IMAGE_NAME}" &>/dev/null && [ "${FORCE_BUILD:-0}" != "1" ]; then
  ok "Image ${IMAGE_NAME} already exists — skipping build (set FORCE_BUILD=1 to override)"
else
  info "Building kafka-operator (Quarkus fast-jar)..."
  cd "${OPERATOR_DIR}"
  mvn package -DskipTests -q
  ok "Build complete"

  info "Building Docker image ${IMAGE_NAME}..."
  docker build -t "${IMAGE_NAME}" "${OPERATOR_DIR}" -q
  ok "Image ${IMAGE_NAME} built"
fi

# ── Step 3: Create Kind clusters (per-cluster CNI configs) ──────────────────
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  config="${SCRIPT_DIR}/${CLUSTER_CONFIGS[$i]}"
  if kind get clusters 2>/dev/null | grep -q "^${cluster}$"; then
    warn "Cluster '${cluster}' already exists — skipping creation"
  else
    info "Creating Kind cluster '${cluster}' (no default CNI, custom CIDRs)..."
    kind create cluster --name "${cluster}" --config "${config}" --wait 60s
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

# ── Step 4: Load image ──────────────────────────────────────────────────────
info "Loading ${IMAGE_NAME} into all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  kind load docker-image "${IMAGE_NAME}" --name "${cluster}"
done
ok "Image loaded into all clusters"

# ── Step 5a: Install CNI plugins (bridge etc.) into each Kind node ──────────
# Kind nodes with disableDefaultCNI only ship flannel/host-local/loopback/portmap/ptp.
# Flannel's CNI shim delegates to 'bridge', which is in the cni-plugins package.
info "Downloading CNI plugins ${CNI_PLUGINS_VERSION}..."
CNI_PLUGINS_URL="https://github.com/containernetworking/plugins/releases/download/${CNI_PLUGINS_VERSION}/cni-plugins-linux-amd64-${CNI_PLUGINS_VERSION}.tgz"
CNI_PLUGINS_TGZ="/tmp/cni-plugins-${CNI_PLUGINS_VERSION}.tgz"
[ -f "${CNI_PLUGINS_TGZ}" ] || curl -sSL "${CNI_PLUGINS_URL}" -o "${CNI_PLUGINS_TGZ}"

info "Installing CNI plugins (bridge, host-local, etc.) into Kind nodes..."
for cluster in "${CLUSTERS[@]}"; do
  # Stream tarball into docker exec stdin — no intermediate file inside the container
  docker exec -i "${cluster}-control-plane" \
    tar -xz -C /opt/cni/bin < "${CNI_PLUGINS_TGZ}"
done
ok "CNI plugins installed into all Kind nodes"

# ── Step 5b: Install Flannel per cluster ────────────────────────────────────
FLANNEL_URL="https://github.com/flannel-io/flannel/releases/download/${FLANNEL_VERSION}/kube-flannel.yml"
info "Downloading Flannel ${FLANNEL_VERSION} manifest..."
FLANNEL_MANIFEST=$(curl -sSL "${FLANNEL_URL}")

for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  subnet="${POD_SUBNETS[$i]}"
  ctx="kind-${cluster}"
  info "Installing Flannel on ${cluster} (podSubnet=${subnet})..."
  echo "${FLANNEL_MANIFEST}" \
    | sed "s|10\.244\.0\.0/16|${subnet}|g" \
    | kubectl --context "${ctx}" apply -f -
done
ok "Flannel installed on all clusters"

# ── Step 5c: Wait for Flannel DaemonSet ready ───────────────────────────────
info "Waiting for Flannel DaemonSet to be Ready on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  kubectl --context "${ctx}" -n kube-flannel rollout status daemonset/kube-flannel-ds --timeout=120s
  ok "Flannel ready on ${cluster}"
done

# ── Step 5d: Enable br_netfilter (required for pod-to-service UDP via iptables NAT) ─
info "Enabling br_netfilter on all Kind nodes..."
for cluster in "${CLUSTERS[@]}"; do
  docker exec "${cluster}-control-plane" bash -c "
    modprobe br_netfilter 2>/dev/null || true
    sysctl -w net.bridge.bridge-nf-call-iptables=1
    sysctl -w net.bridge.bridge-nf-call-ip6tables=1
  "
done
ok "br_netfilter enabled on all nodes"

# ── Step 7: Deploy Submariner broker on cluster A ───────────────────────────
info "Deploying Submariner broker on kafka-a..."
BROKER_INFO="${SCRIPT_DIR}/broker-info.subm"
subctl deploy-broker --context kind-kafka-a
# subctl writes broker-info.subm to CWD; move it to SCRIPT_DIR for reproducibility
[ -f "${PWD}/broker-info.subm" ] && mv "${PWD}/broker-info.subm" "${BROKER_INFO}" || true
ok "Broker deployed (broker-info.subm: ${BROKER_INFO})"

# ── Step 8: Join all clusters to Submariner (in parallel) ───────────────────
info "Joining clusters to Submariner (VXLAN cable driver, no NAT) in parallel..."
SUBCTL_PIDS=()
SUBCTL_LOGS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  log="/tmp/subctl-join-${cluster}.log"
  SUBCTL_LOGS+=("${log}")
  subctl join "${BROKER_INFO}" \
    --context "kind-${cluster}" \
    --clusterid "${cluster}" \
    --natt=false \
    --cable-driver vxlan >"${log}" 2>&1 &
  SUBCTL_PIDS+=($!)
  info "  Joining ${cluster} (pid $!)..."
done

SUBCTL_FAILED=0
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  if wait "${SUBCTL_PIDS[$i]}"; then
    ok "  ${cluster} joined"
  else
    echo "  ✗ ${cluster} join FAILED — output:"
    cat "${SUBCTL_LOGS[$i]}"
    SUBCTL_FAILED=1
  fi
done
[ "${SUBCTL_FAILED}" -eq 0 ] || { echo "Submariner join failed on one or more clusters"; exit 1; }

# ── Step 8b: Patch broker API server address (127.0.0.1 in broker-info.subm
#            is the local kubeconfig port — pods inside clusters can't reach it.
#            Use the kafka-a node's Docker network IP + 6443 instead.) ─────
BROKER_NODE_IP=$(docker inspect kafka-a-control-plane \
  --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
info "Patching Submariner broker API server to ${BROKER_NODE_IP}:6443 on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" patch submariner -n submariner-operator submariner \
    --type merge -p "{\"spec\":{\"brokerK8sApiServer\":\"${BROKER_NODE_IP}:6443\"}}"
done
ok "Broker API server patched"

# ── Step 9: Wait for Submariner gateway + Lighthouse ready ──────────────────
info "Waiting for Submariner gateway + Lighthouse on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  # Pre-wait: kubectl wait fails immediately if no matching pods exist yet
  until kubectl --context "${ctx}" get pod -l app=submariner-gateway \
      -n submariner-operator --no-headers 2>/dev/null | grep -q .; do sleep 3; done
  kubectl --context "${ctx}" wait --for=condition=Ready pod \
    -l app=submariner-gateway -n submariner-operator --timeout=180s
  until kubectl --context "${ctx}" get pod -l app=submariner-lighthouse-agent \
      -n submariner-operator --no-headers 2>/dev/null | grep -q .; do sleep 3; done
  kubectl --context "${ctx}" wait --for=condition=Ready pod \
    -l app=submariner-lighthouse-agent -n submariner-operator --timeout=120s
  ok "Submariner ready on ${cluster}"
done

# ── Step 10: Apply CRDs ──────────────────────────────────────────────────────
info "Applying CRDs to all clusters..."
CRD_DIR="${OPERATOR_DIR}/target/classes/META-INF/fabric8"
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkaclusters.kafka.yavari.afshin.se-v1.yml" --server-side
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkanodepools.kafka.yavari.afshin.se-v1.yml" --server-side
  kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkapodsets.kafka.yavari.afshin.se-v1.yml" --server-side
done
ok "CRDs applied"

# ── Step 11: Namespace + RBAC ────────────────────────────────────────────────
info "Creating namespace '${NAMESPACE}' and RBAC on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/namespace.yaml" --server-side
  kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/rbac.yaml" --server-side
done
ok "Namespace and RBAC ready"

# ── Step 12: Deploy operator (MCS enabled) ───────────────────────────────────
info "Deploying kafka-operator (MCS enabled) to each cluster..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  ctx="kind-${cluster}"
  info "  Deploying to ${cluster} (KAFKA_CLUSTER_ID=${cluster_id}, MCS=true)..."
  KAFKA_CLUSTER_ID="${cluster_id}" KAFKA_NETWORKING_MCS_ENABLED="true" \
    envsubst < "${MANIFESTS_DIR}/operator.yaml" | \
    kubectl --context "${ctx}" apply --server-side -f -
done
ok "Operator deployed with MCS enabled"

# ── Step 13: Apply KafkaCluster CR (clusterset DNS controller addresses) ─────
info "Applying KafkaCluster CR (controllers via svc.clusterset.local)..."
export CTRL_A_ADDR="controllers-a-headless.${NAMESPACE}.svc.clusterset.local:9093"
export CTRL_B_ADDR="controllers-b-headless.${NAMESPACE}.svc.clusterset.local:9093"
export CTRL_C_ADDR="controllers-c-headless.${NAMESPACE}.svc.clusterset.local:9093"

RENDERED_CR=$(mktemp /tmp/kafka-cluster-XXXXXX.yaml)
trap 'rm -f "${RENDERED_CR}"' EXIT
envsubst < "${MANIFESTS_DIR}/kafka-cluster.yaml" > "${RENDERED_CR}"
cat "${RENDERED_CR}"

for cluster in "${CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" apply --server-side -f "${RENDERED_CR}"
done
ok "KafkaCluster CR applied"

# ── Step 14: Apply KafkaNodePools (cluster-prefixed) ────────────────────────
info "Applying KafkaNodePools to all clusters..."
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  CLUSTER_SUFFIX="${cluster_id,,}" envsubst < "${MANIFESTS_DIR}/kafka-node-pools.yaml.tpl" | \
    kubectl --context "kind-${cluster}" apply --server-side -f -
done
ok "KafkaNodePools applied"

# ── Step 15: Wait for Kafka pods Ready ──────────────────────────────────────
info "Waiting for Kafka pods to be Ready on all clusters..."
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  until kubectl --context "${ctx}" -n "${NAMESPACE}" get pods --no-headers 2>/dev/null | grep -q .; do sleep 3; done
  kubectl --context "${ctx}" -n "${NAMESPACE}" wait --for=condition=Ready pod --all --timeout=180s
  ok "Kafka pods ready on ${cluster}"
done

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${GREEN}MCS setup complete!${NC} Submariner is routing cross-cluster traffic."
echo ""
echo "Useful commands:"
echo "  make -C kind status        # check all clusters"
echo "  make -C kind quorum        # verify KRaft quorum"
echo "  make -C kind teardown      # destroy all clusters"
echo "  make -C kind reload-image  # hot-swap operator only (~30s, no cluster rebuild)"
echo "  FORCE_BUILD=1 make -C kind mcs-setup  # force rebuild even if image exists"
echo ""
echo "Verification:"
echo "  kubectl --context kind-kafka-a get serviceexport -n ${NAMESPACE}"
echo "  kubectl --context kind-kafka-b exec -n ${NAMESPACE} brokers-b-0 -- \\"
echo "    nslookup controllers-a-headless.${NAMESPACE}.svc.clusterset.local"
echo "────────────────────────────────────────────────────────────────────────"
