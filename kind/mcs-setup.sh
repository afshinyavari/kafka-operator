#!/bin/bash
# MCS setup: 3 Kind clusters with Flannel CNI + Submariner, kafka-operator with MCS enabled.
# Clusters communicate cross-cluster via *.svc.clusterset.local (Submariner Lighthouse).
# Run 'make -C kind teardown' first if clusters already exist.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPERATOR_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
MANIFESTS_DIR="${SCRIPT_DIR}/manifests"

IMAGE_NAME="kafka-operator:dev"
KAFKA_IMAGE_NAME="kafka-ubi:4.0.0"
KROXY_IMAGE_NAME="kroxy-filters:dev"
APICURIO_PROXY_IMAGE_NAME="apicurio-rbac-proxy:dev"
KAFKA_VERSION="4.0.0"
CLUSTERS=(kafka-a kafka-b kafka-c)
CLUSTER_IDS=(A B C)
# Clusters that host a KafkaProxy. The same KafkaProxy CR is applied to each;
# the operator on every cluster reads spec.targetClusters and only deploys if
# its own KAFKA_CLUSTER_ID is listed.
PROXY_CLUSTERS=(kafka-a kafka-b)
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

# par_wait "label" pids_var logs_var names_var
# Waits for each PID; on failure prints the captured log and exits.
par_wait() {
  local label="$1"
  local -n _pids="$2" _logs="$3" _names="$4"
  local failed=0
  for i in "${!_pids[@]}"; do
    if wait "${_pids[$i]}"; then
      ok "  ${_names[$i]} done"
    else
      echo "  ✗ ${_names[$i]} FAILED — output:"; cat "${_logs[$i]}"; failed=1
    fi
  done
  [ "${failed}" -eq 0 ] || { echo "${label} failed"; exit 1; }
}

# wait_pids "label" pid [pid ...]
# For fast operations where output isn't captured; exits on any failure.
wait_pids() {
  local label="$1"; shift; local failed=0
  for pid in "$@"; do wait "${pid}" || failed=1; done
  [ "${failed}" -eq 0 ] || { echo "ERROR: ${label} failed"; exit 1; }
}

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

# ── Step 1: Build operator (always — regenerates CRD YAMLs in target/kubernetes/) ─
info "Building kafka-operator (Quarkus fast-jar + CRD generation)..."
cd "${OPERATOR_DIR}"
mvn package -DskipTests -q
ok "Build complete"

# ── Step 2: Build Docker image (skip if image already exists) ─────────────────
if docker inspect "${IMAGE_NAME}" &>/dev/null && [ "${FORCE_BUILD:-0}" != "1" ]; then
  ok "Image ${IMAGE_NAME} already exists — skipping Docker build (set FORCE_BUILD=1 to rebuild)"
else
  info "Building Docker image ${IMAGE_NAME}..."
  docker build -t "${IMAGE_NAME}" "${OPERATOR_DIR}" -q
  ok "Image ${IMAGE_NAME} built"
fi

# ── Step 2b: Build Kafka image (skip if already exists) ──────────────────────
if docker inspect "${KAFKA_IMAGE_NAME}" &>/dev/null && [ "${FORCE_BUILD:-0}" != "1" ]; then
  ok "Image ${KAFKA_IMAGE_NAME} already exists — skipping Docker build (set FORCE_BUILD=1 to rebuild)"
else
  info "Building Kafka image ${KAFKA_IMAGE_NAME}..."
  docker build --build-arg KAFKA_VERSION="${KAFKA_VERSION}" \
    -t "${KAFKA_IMAGE_NAME}" "${SCRIPT_DIR}/../kafka-image" -q
  ok "Image ${KAFKA_IMAGE_NAME} built"
fi

# ── Step 2c: Build Kroxylicious filters image (skip if already exists) ────────
if docker inspect "${KROXY_IMAGE_NAME}" &>/dev/null && [ "${FORCE_BUILD:-0}" != "1" ]; then
  ok "Image ${KROXY_IMAGE_NAME} already exists — skipping Docker build (set FORCE_BUILD=1 to rebuild)"
else
  info "Building Kroxylicious filters image ${KROXY_IMAGE_NAME}..."
  cd "${OPERATOR_DIR}/filters" && mvn package -DskipTests -q
  docker build -t "${KROXY_IMAGE_NAME}" "${OPERATOR_DIR}/filters" -q
  ok "Image ${KROXY_IMAGE_NAME} built"
fi

# ── Step 2d: Build apicurio-rbac-proxy image (skip if already exists) ─────────
if docker inspect "${APICURIO_PROXY_IMAGE_NAME}" &>/dev/null && [ "${FORCE_BUILD:-0}" != "1" ]; then
  ok "Image ${APICURIO_PROXY_IMAGE_NAME} already exists — skipping Docker build (set FORCE_BUILD=1 to rebuild)"
else
  info "Building Apicurio RBAC proxy image ${APICURIO_PROXY_IMAGE_NAME}..."
  cd "${OPERATOR_DIR}/apicurio-proxy" && mvn package -DskipTests -q
  docker build -t "${APICURIO_PROXY_IMAGE_NAME}" "${OPERATOR_DIR}/apicurio-proxy" -q
  ok "Image ${APICURIO_PROXY_IMAGE_NAME} built"
fi

# ── Step 3: Create Kind clusters in parallel ─────────────────────────────────
info "Creating Kind clusters in parallel..."
PIDS=(); LOGS=(); NAMES=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  config="${SCRIPT_DIR}/${CLUSTER_CONFIGS[$i]}"
  log="/tmp/kind-create-${cluster}.log"
  LOGS+=("${log}"); NAMES+=("${cluster}")
  (
    if kind get clusters 2>/dev/null | grep -q "^${cluster}$"; then
      echo "Cluster '${cluster}' already exists — skipping creation"
    else
      kind create cluster --name "${cluster}" --config "${config}" --wait 60s
    fi
  ) >"${log}" 2>&1 &
  PIDS+=($!)
done
par_wait "Cluster creation" PIDS LOGS NAMES
ok "All clusters ready"

# ── Step 3b: Label nodes in parallel ─────────────────────────────────────────
info "Labeling Kind nodes with topology.kubernetes.io/zone..."
PIDS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  zone="zone-${CLUSTER_IDS[$i],,}"
  kubectl --context "kind-${cluster}" label node "${cluster}-control-plane" \
    topology.kubernetes.io/zone="${zone}" --overwrite &>/dev/null &
  PIDS+=($!)
done
wait_pids "Node labeling" "${PIDS[@]}"
ok "Nodes labeled"

# ── Step 4: Load images in parallel ──────────────────────────────────────────
info "Loading operator, Kafka, Kroxylicious, and Apicurio RBAC proxy images into all clusters..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  (
    kind load docker-image "${IMAGE_NAME}" --name "${cluster}" &>/dev/null
    kind load docker-image "${KAFKA_IMAGE_NAME}" --name "${cluster}" &>/dev/null
    kind load docker-image "${KROXY_IMAGE_NAME}" --name "${cluster}" &>/dev/null
    kind load docker-image "${APICURIO_PROXY_IMAGE_NAME}" --name "${cluster}" &>/dev/null
  ) &
  PIDS+=($!)
done
wait_pids "Image load" "${PIDS[@]}"
ok "Images loaded into all clusters"

# ── Step 5a: Install CNI plugins in parallel ──────────────────────────────────
# Kind nodes with disableDefaultCNI only ship flannel/host-local/loopback/portmap/ptp.
# Flannel's CNI shim delegates to 'bridge', which is in the cni-plugins package.
info "Downloading CNI plugins ${CNI_PLUGINS_VERSION}..."
CNI_PLUGINS_URL="https://github.com/containernetworking/plugins/releases/download/${CNI_PLUGINS_VERSION}/cni-plugins-linux-amd64-${CNI_PLUGINS_VERSION}.tgz"
CNI_PLUGINS_TGZ="/tmp/cni-plugins-${CNI_PLUGINS_VERSION}.tgz"
[ -f "${CNI_PLUGINS_TGZ}" ] || curl -sSL "${CNI_PLUGINS_URL}" -o "${CNI_PLUGINS_TGZ}"

info "Installing CNI plugins into Kind nodes in parallel..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  # Stream tarball into docker exec stdin — no intermediate file inside the container
  docker exec -i "${cluster}-control-plane" tar -xz -C /opt/cni/bin < "${CNI_PLUGINS_TGZ}" &
  PIDS+=($!)
done
wait_pids "CNI plugin install" "${PIDS[@]}"
ok "CNI plugins installed into all Kind nodes"

# ── Step 5b: Install Flannel in parallel ──────────────────────────────────────
FLANNEL_URL="https://github.com/flannel-io/flannel/releases/download/${FLANNEL_VERSION}/kube-flannel.yml"
info "Downloading Flannel ${FLANNEL_VERSION} manifest..."
FLANNEL_MANIFEST=$(curl -sSL "${FLANNEL_URL}")

info "Installing Flannel on all clusters in parallel..."
PIDS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  subnet="${POD_SUBNETS[$i]}"
  echo "${FLANNEL_MANIFEST}" \
    | sed "s|10\.244\.0\.0/16|${subnet}|g" \
    | kubectl --context "kind-${cluster}" apply -f - &>/dev/null &
  PIDS+=($!)
done
wait_pids "Flannel install" "${PIDS[@]}"
ok "Flannel installed on all clusters"

# ── Step 5c: Wait for Flannel DaemonSet ready in parallel ─────────────────────
info "Waiting for Flannel DaemonSet to be Ready on all clusters..."
PIDS=(); LOGS=(); NAMES=()
for cluster in "${CLUSTERS[@]}"; do
  log="/tmp/flannel-wait-${cluster}.log"
  LOGS+=("${log}"); NAMES+=("${cluster}")
  kubectl --context "kind-${cluster}" -n kube-flannel \
    rollout status daemonset/kube-flannel-ds --timeout=120s >"${log}" 2>&1 &
  PIDS+=($!)
done
par_wait "Flannel wait" PIDS LOGS NAMES
ok "Flannel ready on all clusters"

# ── Step 5d: Enable br_netfilter in parallel ──────────────────────────────────
info "Enabling br_netfilter on all Kind nodes..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  docker exec "${cluster}-control-plane" bash -c "
    modprobe br_netfilter 2>/dev/null || true
    sysctl -w net.bridge.bridge-nf-call-iptables=1
    sysctl -w net.bridge.bridge-nf-call-ip6tables=1
  " &>/dev/null &
  PIDS+=($!)
done
wait_pids "br_netfilter" "${PIDS[@]}"
ok "br_netfilter enabled on all nodes"

# ── Step 7: Deploy Submariner broker on cluster A ─────────────────────────────
info "Deploying Submariner broker on kafka-a..."
BROKER_INFO="${SCRIPT_DIR}/broker-info.subm"
subctl deploy-broker --context kind-kafka-a
# subctl writes broker-info.subm to CWD; move it to SCRIPT_DIR for reproducibility
[ -f "${PWD}/broker-info.subm" ] && mv "${PWD}/broker-info.subm" "${BROKER_INFO}" || true
ok "Broker deployed (broker-info.subm: ${BROKER_INFO})"

# ── Step 8: Join all clusters to Submariner in parallel ───────────────────────
info "Joining clusters to Submariner (VXLAN cable driver, no NAT) in parallel..."
PIDS=(); LOGS=(); NAMES=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  log="/tmp/subctl-join-${cluster}.log"
  LOGS+=("${log}"); NAMES+=("${cluster}")
  subctl join "${BROKER_INFO}" \
    --context "kind-${cluster}" \
    --clusterid "${cluster}" \
    --natt=false \
    --cable-driver vxlan >"${log}" 2>&1 &
  PIDS+=($!)
  info "  Joining ${cluster} (pid $!)..."
done
par_wait "Submariner join" PIDS LOGS NAMES

# ── Step 8b: Patch broker API server in parallel ──────────────────────────────
# 127.0.0.1 in broker-info.subm is the local kubeconfig port — pods inside
# clusters can't reach it. Use the kafka-a node's Docker network IP + 6443.
BROKER_NODE_IP=$(docker inspect kafka-a-control-plane \
  --format '{{.NetworkSettings.Networks.kind.IPAddress}}')
info "Patching Submariner broker API server to ${BROKER_NODE_IP}:6443 on all clusters..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" patch submariner -n submariner-operator submariner \
    --type merge -p "{\"spec\":{\"brokerK8sApiServer\":\"${BROKER_NODE_IP}:6443\"}}" &>/dev/null &
  PIDS+=($!)
done
wait_pids "Broker patch" "${PIDS[@]}"
ok "Broker API server patched"

# ── Step 9: Wait for Submariner gateway + Lighthouse in parallel ───────────────
info "Waiting for Submariner gateway + Lighthouse on all clusters..."
PIDS=(); LOGS=(); NAMES=()
for cluster in "${CLUSTERS[@]}"; do
  log="/tmp/submariner-wait-${cluster}.log"
  LOGS+=("${log}"); NAMES+=("${cluster}")
  (
    ctx="kind-${cluster}"
    until kubectl --context "${ctx}" get pod -l app=submariner-gateway \
        -n submariner-operator --no-headers 2>/dev/null | grep -q .; do sleep 3; done
    kubectl --context "${ctx}" wait --for=condition=Ready pod \
      -l app=submariner-gateway -n submariner-operator --timeout=180s
    until kubectl --context "${ctx}" get pod -l app=submariner-lighthouse-agent \
        -n submariner-operator --no-headers 2>/dev/null | grep -q .; do sleep 3; done
    kubectl --context "${ctx}" wait --for=condition=Ready pod \
      -l app=submariner-lighthouse-agent -n submariner-operator --timeout=120s
  ) >"${log}" 2>&1 &
  PIDS+=($!)
done
par_wait "Submariner wait" PIDS LOGS NAMES
ok "Submariner ready on all clusters"

# ── Step 10: Apply CRDs in parallel ──────────────────────────────────────────
info "Applying CRDs to all clusters..."
CRD_DIR="${OPERATOR_DIR}/target/kubernetes"
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  (
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkaclusters.kafka.yavari.afshin.se-v1.yml" --server-side
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkanodepools.kafka.yavari.afshin.se-v1.yml" --server-side
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkapodsets.kafka.yavari.afshin.se-v1.yml" --server-side
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkarbacs.kafka.yavari.afshin.se-v1.yml" --server-side
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/kafkaproxies.kafka.yavari.afshin.se-v1.yml" --server-side
    kubectl --context "${ctx}" apply -f "${CRD_DIR}/apicurioregistries.kafka.yavari.afshin.se-v1.yml" --server-side
  ) &>/dev/null &
  PIDS+=($!)
done
wait_pids "CRD apply" "${PIDS[@]}"
ok "CRDs applied"

# ── Step 11: Namespace + RBAC in parallel ─────────────────────────────────────
info "Creating namespace '${NAMESPACE}' and RBAC on all clusters..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  (
    kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/namespace.yaml" --server-side
    kubectl --context "${ctx}" apply -f "${MANIFESTS_DIR}/rbac.yaml" --server-side
  ) &>/dev/null &
  PIDS+=($!)
done
wait_pids "Namespace + RBAC" "${PIDS[@]}"
ok "Namespace and RBAC ready"

# ── Step 11b: Mint full mTLS cert set + distribute as Secrets ─────────────────
# Stands in for cert-manager in this test rig. Generates ONE CA locally, signs all
# leaf certs (per-pool broker, proxy client, proxy server, test client), and applies
# each as a kubernetes.io/tls-shaped Secret (tls.crt + tls.key + ca.crt — cert-manager
# convention) to the clusters that need it. The operator no longer signs anything;
# it just mounts the named secrets and reschedules until they exist.

CA_DIR=$(mktemp -d /tmp/kafka-mtls-ca-XXXXXX)
trap 'rm -rf "${CA_DIR}"' EXIT
PROXY_PRINCIPAL="kafka-proxy"

info "Generating local CA and leaf certs (CA, brokers-{a,b,c}, proxy client/server, test client)..."
# CA
openssl genrsa -out "${CA_DIR}/ca.key" 2048 2>/dev/null
openssl req -x509 -new -nodes -key "${CA_DIR}/ca.key" \
  -subj "/CN=kafka-operator-ca" -days 3650 \
  -out "${CA_DIR}/ca.crt" 2>/dev/null

# Helper: sign a leaf cert and convert key to PKCS#8 PEM (Kroxylicious/Netty needs PKCS#8).
# $1=name (file prefix), $2=CN, $3=SAN list (comma-separated dnsNames, or empty for no SAN)
mint_cert() {
  local name="$1" cn="$2" sans="$3"
  openssl genrsa -out "${CA_DIR}/${name}.key.raw" 2048 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "${CA_DIR}/${name}.key.raw" -out "${CA_DIR}/${name}.key" 2>/dev/null
  local extfile="${CA_DIR}/${name}.ext"
  if [ -n "${sans}" ]; then
    {
      echo "basicConstraints=CA:FALSE"
      echo "keyUsage=digitalSignature,keyEncipherment"
      echo "extendedKeyUsage=serverAuth,clientAuth"
      echo -n "subjectAltName="
      local first=1
      IFS=',' read -ra SAN_ARR <<< "${sans}"
      for s in "${SAN_ARR[@]}"; do
        if [ ${first} -eq 1 ]; then first=0; else echo -n ","; fi
        echo -n "DNS:${s}"
      done
      echo ""
    } > "${extfile}"
  else
    {
      echo "basicConstraints=CA:FALSE"
      echo "keyUsage=digitalSignature,keyEncipherment"
      echo "extendedKeyUsage=clientAuth"
    } > "${extfile}"
  fi
  openssl req -new -key "${CA_DIR}/${name}.key" -subj "/CN=${cn}" \
    -out "${CA_DIR}/${name}.csr" 2>/dev/null
  openssl x509 -req -in "${CA_DIR}/${name}.csr" \
    -CA "${CA_DIR}/ca.crt" -CAkey "${CA_DIR}/ca.key" -CAcreateserial \
    -out "${CA_DIR}/${name}.crt" -days 3650 -sha256 \
    -extfile "${extfile}" 2>/dev/null
}

# Per-pool broker certs (one per cluster), each with both cluster.local + clusterset.local SANs.
for cluster in "${CLUSTERS[@]}"; do
  suffix="${cluster#kafka-}"  # a / b / c
  pool="brokers-${suffix}"
  mint_cert "${pool}" "${pool}" \
    "${pool}-headless.${NAMESPACE}.svc.cluster.local,${pool}-headless.${NAMESPACE}.svc.clusterset.local"
done

# Proxy client cert — CN goes into broker super.users.
mint_cert "${PROXY_PRINCIPAL}-client" "${PROXY_PRINCIPAL}" ""
# Proxy server cert — presented to clients; SANs match the gateway Service hostname.
mint_cert "${PROXY_PRINCIPAL}-server" "${PROXY_PRINCIPAL}" \
  "${PROXY_PRINCIPAL}.${NAMESPACE}.svc.cluster.local,${PROXY_PRINCIPAL}.${NAMESPACE}.svc.clusterset.local"
# Test client cert — extracted by proxy-test.sh / rbac-test.sh for SASL_SSL mTLS handshake.
mint_cert "${PROXY_PRINCIPAL}-test-client" "test-client" ""

# Helper: apply a 3-key kubernetes.io/tls-style secret (cert-manager convention).
apply_tls_secret() {
  local ctx="$1" secret_name="$2" crt="$3" key="$4"
  kubectl --context "${ctx}" -n "${NAMESPACE}" create secret generic "${secret_name}" \
    --type=kubernetes.io/tls \
    --from-file=tls.crt="${crt}" \
    --from-file=tls.key="${key}" \
    --from-file=ca.crt="${CA_DIR}/ca.crt" \
    --dry-run=client -o yaml | \
    kubectl --context "${ctx}" apply --server-side -f -
}

info "Applying broker-pool TLS secrets to each cluster..."
PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  ctx="kind-${cluster}"
  suffix="${cluster#kafka-}"
  pool="brokers-${suffix}"
  (
    apply_tls_secret "${ctx}" "${pool}-broker-tls" "${CA_DIR}/${pool}.crt" "${CA_DIR}/${pool}.key"
  ) &>/dev/null &
  PIDS+=($!)
done
wait_pids "Broker TLS secrets" "${PIDS[@]}"

info "Applying proxy client/server/test-client TLS secrets to: ${PROXY_CLUSTERS[*]}..."
for cluster in "${PROXY_CLUSTERS[@]}"; do
  PROXY_CTX="kind-${cluster}"
  apply_tls_secret "${PROXY_CTX}" "${PROXY_PRINCIPAL}-client-tls" \
    "${CA_DIR}/${PROXY_PRINCIPAL}-client.crt" "${CA_DIR}/${PROXY_PRINCIPAL}-client.key" &>/dev/null
  apply_tls_secret "${PROXY_CTX}" "${PROXY_PRINCIPAL}-server-tls" \
    "${CA_DIR}/${PROXY_PRINCIPAL}-server.crt" "${CA_DIR}/${PROXY_PRINCIPAL}-server.key" &>/dev/null
  apply_tls_secret "${PROXY_CTX}" "${PROXY_PRINCIPAL}-test-client-tls" \
    "${CA_DIR}/${PROXY_PRINCIPAL}-test-client.crt" "${CA_DIR}/${PROXY_PRINCIPAL}-test-client.key" &>/dev/null
done
ok "All mTLS secrets provisioned"

# ── Step 12: Deploy operator in parallel ──────────────────────────────────────
info "Deploying kafka-operator (MCS enabled) to all clusters..."
PIDS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  ctx="kind-${cluster}"
  KAFKA_CLUSTER_ID="${cluster_id}" KAFKA_NETWORKING_MCS_ENABLED="true" \
    envsubst < "${MANIFESTS_DIR}/operator.yaml" | \
    kubectl --context "${ctx}" apply --server-side -f - &>/dev/null &
  PIDS+=($!)
done
wait_pids "Operator deploy" "${PIDS[@]}"
ok "Operator deployed with MCS enabled"

# ── Step 12b: Export per-cluster operator service for cross-cluster roll coordination ──
info "Exporting operator HTTP service for cross-cluster roll order checks..."
PIDS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  CLUSTER_ID_LOWER="${cluster_id,,}" envsubst < "${MANIFESTS_DIR}/operator-service-export.yaml" | \
    kubectl --context "kind-${cluster}" apply --server-side -f - &>/dev/null &
  PIDS+=($!)
done
wait_pids "Operator ServiceExport" "${PIDS[@]}"
ok "Operator services exported (kafka-operator-{a,b,c}.kafka.svc.clusterset.local:8080)"

# ── Step 13: Apply KafkaCluster CR in parallel ────────────────────────────────
info "Applying KafkaCluster CR (controllers via svc.clusterset.local)..."
export CTRL_A_ADDR="controllers-a-headless.${NAMESPACE}.svc.clusterset.local:9093"
export CTRL_B_ADDR="controllers-b-headless.${NAMESPACE}.svc.clusterset.local:9093"
export CTRL_C_ADDR="controllers-c-headless.${NAMESPACE}.svc.clusterset.local:9093"

RENDERED_CR=$(mktemp /tmp/kafka-cluster-XXXXXX.yaml)
trap 'rm -f "${RENDERED_CR}"' EXIT
envsubst < "${MANIFESTS_DIR}/kafka-cluster.yaml" > "${RENDERED_CR}"

PIDS=()
for cluster in "${CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" apply --server-side -f "${RENDERED_CR}" &>/dev/null &
  PIDS+=($!)
done
wait_pids "KafkaCluster CR" "${PIDS[@]}"
ok "KafkaCluster CR applied"

# ── Step 14: Apply KafkaNodePools in parallel ─────────────────────────────────
info "Applying KafkaNodePools to all clusters..."
PIDS=()
for i in "${!CLUSTERS[@]}"; do
  cluster="${CLUSTERS[$i]}"
  cluster_id="${CLUSTER_IDS[$i]}"
  CLUSTER_SUFFIX="${cluster_id,,}" envsubst < "${MANIFESTS_DIR}/kafka-node-pools.yaml.tpl" | \
    kubectl --context "kind-${cluster}" apply --server-side -f - &>/dev/null &
  PIDS+=($!)
done
wait_pids "KafkaNodePools" "${PIDS[@]}"
ok "KafkaNodePools applied"

# ── Step 15: Wait for Kafka pods Ready in parallel ────────────────────────────
info "Waiting for Kafka pods to be Ready on all clusters..."
PIDS=(); LOGS=(); NAMES=()
for cluster in "${CLUSTERS[@]}"; do
  log="/tmp/kafka-pods-wait-${cluster}.log"
  LOGS+=("${log}"); NAMES+=("${cluster}")
  (
    ctx="kind-${cluster}"
    until kubectl --context "${ctx}" -n "${NAMESPACE}" get pods --no-headers 2>/dev/null | grep -q .; do sleep 3; done
    kubectl --context "${ctx}" -n "${NAMESPACE}" wait --for=condition=Ready pod --all --timeout=180s
  ) >"${log}" 2>&1 &
  PIDS+=($!)
done
par_wait "Kafka pods wait" PIDS LOGS NAMES
ok "Kafka pods ready on all clusters"

# ── Step 16: Deploy Keycloak on cluster-a ─────────────────────────────────────
info "Deploying Keycloak on kafka-a..."
kubectl --context kind-kafka-a apply -f "${MANIFESTS_DIR}/keycloak.yaml" --server-side &>/dev/null
info "Waiting for Keycloak to be Ready (up to 5 min)..."
kubectl --context kind-kafka-a -n "${NAMESPACE}" wait --for=condition=Ready \
  pod -l app=keycloak --timeout=300s
ok "Keycloak ready"

# ── Step 17: Deploy KafkaRbac + ApicurioRegistry + KafkaProxy ──────────────────
# Order matters: ApicurioRegistry needs the KafkaRbac-generated policy ConfigMap,
# and the KafkaProxy reads ApicurioRegistry.status.registryUrl to wire the
# record-validation filter, so deploy in dependency order to avoid status churn.
info "Deploying KafkaRbac on: ${PROXY_CLUSTERS[*]}..."
for cluster in "${PROXY_CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" apply -f "${MANIFESTS_DIR}/kafkarbac.yaml" --server-side &>/dev/null
done

info "Deploying ApicurioRegistry (registry + rbac-proxy) on kafka-a..."
kubectl --context kind-kafka-a apply -f "${MANIFESTS_DIR}/apicurioregistry.yaml" --server-side &>/dev/null
info "Waiting for ApicurioRegistry to reach READY (up to 5 min)..."
until kubectl --context kind-kafka-a -n "${NAMESPACE}" get apicurioregistry apicurio \
    -o jsonpath='{.status.phase}' 2>/dev/null | grep -q READY; do sleep 5; done
ok "ApicurioRegistry READY"

info "Deploying KafkaProxy CR to: ${PROXY_CLUSTERS[*]} (each operator decides whether to deploy locally based on spec.targetClusters)..."
for cluster in "${PROXY_CLUSTERS[@]}"; do
  kubectl --context "kind-${cluster}" apply -f "${MANIFESTS_DIR}/kafkaproxy.yaml" --server-side &>/dev/null
done
info "Waiting for KafkaProxy to reach READY on each target cluster (up to 5 min)..."
for cluster in "${PROXY_CLUSTERS[@]}"; do
  until kubectl --context "kind-${cluster}" -n "${NAMESPACE}" get kafkaproxy kafka-proxy \
      -o jsonpath='{.status.phase}' 2>/dev/null | grep -q READY; do sleep 5; done
  ok "KafkaProxy READY on ${cluster}"
done

echo ""
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${GREEN}MCS setup complete!${NC} Submariner is routing cross-cluster traffic."
echo ""
echo "Useful commands:"
echo "  make -C kind status        # check all clusters"
echo "  make -C kind quorum        # verify KRaft quorum"
echo "  make -C kind proxy-test               # quick mTLS + OIDC produce/consume test"
echo "  make -C kind rbac-test                # RBAC allow/deny test via mTLS + OIDC"
echo "  make -C kind apicurio-rbac-test       # Apicurio rbac-proxy HTTP authz test"
echo "  make -C kind xml-filter-test          # XML validation filter accept/reject test"
echo "  make -C kind teardown                 # destroy all clusters"
echo "  make -C kind reload-image  # hot-swap operator only (~30s, no cluster rebuild)"
echo "  FORCE_BUILD=1 make -C kind mcs-setup  # force rebuild even if image exists"
echo ""
echo "Verification:"
echo "  kubectl --context kind-kafka-a get serviceexport -n ${NAMESPACE}"
echo "  kubectl --context kind-kafka-b exec -n ${NAMESPACE} brokers-b-0 -- \\"
echo "    nslookup controllers-a-headless.${NAMESPACE}.svc.clusterset.local"
echo "────────────────────────────────────────────────────────────────────────"
