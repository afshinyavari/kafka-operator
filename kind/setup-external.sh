#!/bin/bash
# Install MetalLB + Envoy Gateway + nginx-ingress on the three Kind clusters so the
# KafkaProxy external-access tests (proxy-external-{lb,gateway,ingress}-test.sh) can run.
#
# MetalLB advertises LoadBalancer IPs on the Kind docker bridge (172.19.0.0/16 on this host).
# Each cluster gets a non-overlapping /28-ish slice high in the subnet so the IPs don't
# collide with the kind node IPs (172.19.0.2-4) or anything Submariner allocates.
#
# Usage:
#   ./setup-external.sh           # install everything on all 3 clusters
#   ./setup-external.sh metallb   # MetalLB only
#   ./setup-external.sh gateway   # Envoy Gateway only
#   ./setup-external.sh ingress   # nginx-ingress only

set -euo pipefail
export PATH="${HOME}/.local/bin:${PATH}"

CLUSTERS=(kafka-a kafka-b kafka-c)
METALLB_VERSION="v0.14.8"
ENVOY_GATEWAY_VERSION="v1.2.4"
NGINX_INGRESS_VERSION="v1.11.3"

# Non-overlapping /28-ish ranges on the kind docker bridge (172.19.0.0/16).
# Confirmed reachable from the host (docker0 / kind bridge routing).
declare -A METALLB_POOL
METALLB_POOL[kafka-a]="172.19.255.200-172.19.255.210"
METALLB_POOL[kafka-b]="172.19.255.220-172.19.255.230"
METALLB_POOL[kafka-c]="172.19.255.240-172.19.255.250"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }

install_metallb() {
  for cluster in "${CLUSTERS[@]}"; do
    info "MetalLB on ${cluster} → pool ${METALLB_POOL[$cluster]}"
    kubectl --context "kind-${cluster}" apply -f \
      "https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"

    info "Waiting for MetalLB controller on ${cluster}..."
    kubectl --context "kind-${cluster}" -n metallb-system \
      wait --for=condition=Available deployment/controller --timeout=180s

    info "Waiting for MetalLB webhook endpoints on ${cluster} (race-prone)..."
    # MetalLB's IPAddressPool admission webhook 503s until the webhook pod has endpoints.
    for _ in {1..60}; do
      ENDPOINTS=$(kubectl --context "kind-${cluster}" -n metallb-system \
        get endpoints metallb-webhook-service -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || echo "")
      [ -n "${ENDPOINTS}" ] && break
      sleep 1
    done

    # IPAddressPool + L2Advertisement
    cat <<EOF | kubectl --context "kind-${cluster}" apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kafka-pool
  namespace: metallb-system
spec:
  addresses:
    - ${METALLB_POOL[$cluster]}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kafka-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - kafka-pool
EOF
    ok "MetalLB ready on ${cluster}"
  done
}

install_envoy_gateway() {
  for cluster in "${CLUSTERS[@]}"; do
    info "Envoy Gateway on ${cluster}"
    kubectl --context "kind-${cluster}" apply --server-side -f \
      "https://github.com/envoyproxy/gateway/releases/download/${ENVOY_GATEWAY_VERSION}/install.yaml"

    info "Waiting for Envoy Gateway control plane on ${cluster}..."
    kubectl --context "kind-${cluster}" -n envoy-gateway-system \
      wait --for=condition=Available deployment/envoy-gateway --timeout=180s

    # GatewayClass + Gateway with TLS passthrough listener on port 9094.
    # NOTE: TLSRoute is the v1alpha2 namespaced kind; Gateway listener uses mode=Passthrough so
    # the proxy terminates TLS, not Envoy.
    cat <<EOF | kubectl --context "kind-${cluster}" apply -f -
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: envoy
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: kafka-gateway
  namespace: kafka
spec:
  gatewayClassName: envoy
  listeners:
    - name: tls-passthrough
      protocol: TLS
      port: 9094
      tls:
        mode: Passthrough
      allowedRoutes:
        kinds:
          - kind: TLSRoute
        namespaces:
          from: All
EOF
    ok "Envoy Gateway ready on ${cluster}"
  done
}

install_nginx_ingress() {
  for cluster in "${CLUSTERS[@]}"; do
    info "nginx-ingress on ${cluster}"
    kubectl --context "kind-${cluster}" apply -f \
      "https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-${NGINX_INGRESS_VERSION}/deploy/static/provider/cloud/deploy.yaml"

    # Patch the controller to enable SSL passthrough — required because Kafka clients
    # do TLS to the proxy directly; nginx must forward raw TLS, not terminate.
    info "Patching nginx-ingress controller to --enable-ssl-passthrough on ${cluster}..."
    kubectl --context "kind-${cluster}" -n ingress-nginx patch deployment ingress-nginx-controller \
      --type='json' -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--enable-ssl-passthrough"}]'

    info "Waiting for nginx-ingress controller on ${cluster}..."
    kubectl --context "kind-${cluster}" -n ingress-nginx \
      wait --for=condition=Available deployment/ingress-nginx-controller --timeout=240s
    ok "nginx-ingress ready on ${cluster}"
  done
}

case "${1:-all}" in
  metallb)  install_metallb ;;
  gateway)  install_envoy_gateway ;;
  ingress)  install_nginx_ingress ;;
  all)
    install_metallb
    install_envoy_gateway
    install_nginx_ingress
    ;;
  *) echo "Usage: $0 [metallb|gateway|ingress|all]"; exit 1 ;;
esac

echo ""
ok "External-access infrastructure installed. Next:"
echo "  - make reload-image           # reload operator with externalAccess support"
echo "  - make proxy-external-lb-test"
echo "  - make proxy-external-gateway-test"
echo "  - make proxy-external-ingress-test"
