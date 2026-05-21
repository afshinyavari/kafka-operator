SHELL              := /bin/bash
export PATH        := $(HOME)/.local/bin:$(PATH)
CLUSTERS           := kafka-a kafka-b kafka-c
NAMESPACE          := kafka
IMAGE              := kafka-operator:dev
KAFKA_IMAGE        := kafka-ubi:4.0.0
KAFKA_VERSION      := 4.0.0
KROXY_IMAGE        := kroxy-filters:dev
APICURIO_PROXY_IMG := apicurio-rbac-proxy:dev
KAFKA_UI_IMG       := kafka-ui:dev
OPERATOR_DIR       := ..
MANIFESTS_DIR      := manifests

.PHONY: all setup mcs-setup teardown status quorum proxy-test xml-filter-test \
        logs-a logs-b logs-c build image \
        kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
        kafka-ui-image reload-kafka-ui-image kafka-ui-setup ui-test \
        keycloak-setup proxy-setup rbac-test apicurio-setup apicurio-rbac-test \
        setup-external proxy-external-lb-test proxy-external-gateway-test \
        proxy-external-ingress-test help \
        kafkatopic-test json-schema-test durability-test rolling-restart-test \
        broker-kill-test e2e

all: setup  ## Full setup: build → image → clusters → deploy

setup:  ## Create clusters, build + load image, deploy operator and Kafka CRs (NodePort mode)
	@chmod +x setup.sh && ./setup.sh

mcs-setup:  ## Full MCS setup: Flannel + Submariner + operator with kafka.networking.mcs-enabled=true
	@chmod +x mcs-setup.sh && ./mcs-setup.sh

teardown:  ## Destroy all Kind clusters
	@chmod +x teardown.sh && ./teardown.sh

status:  ## Show operator/pod/CR status on all clusters
	@chmod +x status.sh && ./status.sh

quorum:  ## Verify KRaft controller quorum health
	@chmod +x quorum.sh && ./quorum.sh

proxy-test:  ## Quick end-to-end produce/consume test through proxy (mTLS + OIDC)
	@chmod +x proxy-test.sh && ./proxy-test.sh

mcs-proxy-test:  ## Cross-cluster proxy test: produce/consume from kafka-b via clusterset.local
	@chmod +x mcs-proxy-test.sh && ./mcs-proxy-test.sh

setup-external:  ## Install MetalLB + Envoy Gateway + nginx-ingress on all Kind clusters
	@chmod +x setup-external.sh && ./setup-external.sh

proxy-external-lb-test:  ## E2E: KafkaProxy externalAccess.type=LOADBALANCER (MetalLB)
	@chmod +x proxy-external-lb-test.sh && ./proxy-external-lb-test.sh

proxy-external-gateway-test:  ## E2E: KafkaProxy externalAccess.type=GATEWAY (Envoy Gateway TLSRoute)
	@chmod +x proxy-external-gateway-test.sh && ./proxy-external-gateway-test.sh

proxy-external-ingress-test:  ## E2E: KafkaProxy externalAccess.type=INGRESS (nginx ssl-passthrough)
	@chmod +x proxy-external-ingress-test.sh && ./proxy-external-ingress-test.sh

xml-filter-test:  ## End-to-end test of the XML validation filter (valid + invalid XML)
	@chmod +x xml-filter-test.sh && ./xml-filter-test.sh

build:  ## Build the kafka-operator Maven package (skips tests)
	cd $(OPERATOR_DIR) && mvn package -DskipTests -q

image: build  ## Build the kafka-operator Docker image
	docker build -t $(IMAGE) $(OPERATOR_DIR)

# ── Per-cluster operator log tails ──────────────────────────────────────────
logs-a:  ## Tail operator logs on cluster A
	kubectl --context kind-kafka-a -n $(NAMESPACE) logs -l app=kafka-operator -f

logs-b:  ## Tail operator logs on cluster B
	kubectl --context kind-kafka-b -n $(NAMESPACE) logs -l app=kafka-operator -f

logs-c:  ## Tail operator logs on cluster C
	kubectl --context kind-kafka-c -n $(NAMESPACE) logs -l app=kafka-operator -f

# ── Convenience per-cluster kubectl shortcuts ────────────────────────────────
pods-a:  ## List Kafka pods on cluster A
	kubectl --context kind-kafka-a -n $(NAMESPACE) get pods -l kafka.yavari.afshin.se/cluster=my-kafka -o wide

pods-b:
	kubectl --context kind-kafka-b -n $(NAMESPACE) get pods -l kafka.yavari.afshin.se/cluster=my-kafka -o wide

pods-c:
	kubectl --context kind-kafka-c -n $(NAMESPACE) get pods -l kafka.yavari.afshin.se/cluster=my-kafka -o wide

cr-status:  ## Show KafkaCluster CR status on all clusters
	@for cluster in $(CLUSTERS); do \
	  echo "=== $$cluster ==="; \
	  kubectl --context kind-$$cluster -n $(NAMESPACE) get kafkacluster my-kafka -o jsonpath='{.status}' 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(not found)"; \
	  echo ""; \
	done

kafka-image:  ## Build the UBI9-based Kafka image
	docker build --build-arg KAFKA_VERSION=$(KAFKA_VERSION) \
	  -t $(KAFKA_IMAGE) $(OPERATOR_DIR)/kafka-image

reload-kafka-image:  ## Rebuild + reload Kafka image into Kind clusters
	$(MAKE) kafka-image
	@for cluster in $(CLUSTERS); do \
	  kind load docker-image $(KAFKA_IMAGE) --name $$cluster; \
	done

reload-image:  ## Rebuild + reload image without recreating clusters
	$(MAKE) image
	@for cluster in $(CLUSTERS); do \
	  kind load docker-image $(IMAGE) --name $$cluster; \
	  kubectl --context kind-$$cluster -n $(NAMESPACE) rollout restart deployment kafka-operator; \
	done

kroxy-image:  ## Build the Kroxylicious + custom filters Docker image
	cd $(OPERATOR_DIR)/filters && mvn package -DskipTests -q
	docker build -t $(KROXY_IMAGE) $(OPERATOR_DIR)/filters

reload-kroxy-image: kroxy-image  ## Rebuild + reload kroxy-filters image into all Kind clusters
	@for cluster in $(CLUSTERS); do \
	  kind load docker-image $(KROXY_IMAGE) --name $$cluster; \
	done

apicurio-proxy-image:  ## Build the apicurio-rbac-proxy Docker image
	cd $(OPERATOR_DIR)/apicurio-proxy && mvn package -DskipTests -q
	docker build -t $(APICURIO_PROXY_IMG) $(OPERATOR_DIR)/apicurio-proxy

reload-apicurio-proxy-image: apicurio-proxy-image  ## Rebuild + reload apicurio-rbac-proxy image into all Kind clusters
	@for cluster in $(CLUSTERS); do \
	  kind load docker-image $(APICURIO_PROXY_IMG) --name $$cluster; \
	done

kafka-ui-image:  ## Build the kafka-ui Docker image
	cd $(OPERATOR_DIR)/kafka-ui && mvn package -DskipTests -q
	docker build -t $(KAFKA_UI_IMG) $(OPERATOR_DIR)/kafka-ui

reload-kafka-ui-image: kafka-ui-image  ## Rebuild + reload kafka-ui image into kafka-a (UI is single-instance)
	kind load docker-image $(KAFKA_UI_IMG) --name kafka-a
	-kubectl --context kind-kafka-a -n $(NAMESPACE) rollout restart deployment kafka-ui

kafka-ui-setup:  ## Deploy kafka-ui to kafka-a via KafkaUI CR (operator-reconciled)
	$(MAKE) reload-kafka-ui-image
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafka-ui-oidc-secret.yaml
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafka-ui-cr.yaml
	@echo "Waiting for operator to create the kafka-ui Deployment..."
	@for i in $$(seq 1 60); do \
	  kubectl --context kind-kafka-a -n $(NAMESPACE) get deployment kafka-ui >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	@echo "Waiting for kafka-ui rollout to converge (old pods terminated)..."
	kubectl --context kind-kafka-a -n $(NAMESPACE) rollout status deployment/kafka-ui --timeout=120s
	kubectl --context kind-kafka-a -n $(NAMESPACE) wait --for=condition=Ready \
	  pod -l app.instance=kafka-ui --timeout=120s
	@echo "kafka-ui ready. URL: $$(kubectl --context kind-kafka-a -n $(NAMESPACE) get kafkaui kafka-ui -o jsonpath='{.status.advertisedHost}' 2>/dev/null || true)"
	@echo "  (if blank, status.advertisedHost is empty — externalAccess.type may be NODEPORT/internal)"

ui-test:  ## End-to-end UI smoke test (login + cluster listing + topic browse)
	@chmod +x ui-test.sh && ./ui-test.sh

KEYCLOAK_EXTERNAL ?= loadbalancer
KEYCLOAK_HOST ?= keycloak.kafka.svc.clusterset.local

keycloak-setup:  ## Deploy Keycloak to cluster-a (namespace: kafka) and wait for Ready
	@sed -e "s|__KEYCLOAK_HOST__|$(KEYCLOAK_HOST)|g" $(MANIFESTS_DIR)/keycloak.yaml \
	  | kubectl --context kind-kafka-a apply --server-side -f -
	@for mode in loadbalancer ingress gateway; do \
	  if [ "$(KEYCLOAK_EXTERNAL)" != "$$mode" ]; then \
	    kubectl --context kind-kafka-a -n $(NAMESPACE) delete \
	      service,ingress,httproute.gateway.networking.k8s.io \
	      -l "external-access-mode=$$mode" --ignore-not-found >/dev/null 2>&1 || true; \
	  fi; \
	done
	@echo "Waiting for Keycloak to be Ready (up to 5 min)..."
	kubectl --context kind-kafka-a -n $(NAMESPACE) wait --for=condition=Ready \
	  pod -l app=keycloak --timeout=300s
	@echo "Keycloak ready (external=$(KEYCLOAK_EXTERNAL))."

proxy-setup:  ## Apply KafkaProxy + KafkaRbac CRs to cluster-a, wait for READY
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafkarbac.yaml
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafkaproxy.yaml
	@echo "Waiting for KafkaProxy to reach READY phase..."
	@until kubectl --context kind-kafka-a -n $(NAMESPACE) get kafkaproxy kafka-proxy \
	    -o jsonpath='{.status.phase}' 2>/dev/null | grep -q READY; do \
	  sleep 5; \
	done
	@echo "KafkaProxy READY."

rbac-test:  ## Run end-to-end RBAC test via SASL/OAUTHBEARER
	@chmod +x rbac-test.sh && ./rbac-test.sh

apicurio-setup:  ## Build + deploy Apicurio Registry (kafkasql) + RBAC proxy to cluster-a
	$(MAKE) reload-apicurio-proxy-image
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafkarbac.yaml
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/apicurio-kafkasql.yaml
	@echo "Waiting for ApicurioRegistry to be Ready..."
	@until kubectl --context kind-kafka-a -n $(NAMESPACE) get apicurioregistry apicurio \
	    -o jsonpath='{.status.phase}' 2>/dev/null | grep -q READY; do sleep 5; done
	@echo "ApicurioRegistry READY."

apicurio-rbac-test:  ## Run end-to-end schema registry RBAC test
	@chmod +x apicurio-rbac-test.sh && ./apicurio-rbac-test.sh

# ── New e2e sub-tests ─────────────────────────────────────────────────────────
kafkatopic-test:  ## KafkaTopic CR lifecycle: create, alter, DELETE vs RETAIN
	@chmod +x kafkatopic-test.sh && ./kafkatopic-test.sh

json-schema-test:  ## Apicurio JSON schema + produce/consume through KafkaProxy LB
	@chmod +x json-schema-test.sh && ./json-schema-test.sh

durability-test:  ## kafkasql: schema survives Apicurio pod restart
	@chmod +x durability-test.sh && ./durability-test.sh

rolling-restart-test:  ## Rolling broker restart preserves quorum + no message loss
	@chmod +x rolling-restart-test.sh && ./rolling-restart-test.sh

broker-kill-test:  ## Kill a broker; producers/consumers keep going; broker rejoins ISR
	@chmod +x broker-kill-test.sh && ./broker-kill-test.sh

# ── Unified e2e: one target that exercises every shipped feature ──────────────
# Composes the existing tests in dependency order, including the new ones above.
# Assumes mcs-setup is already done (it bakes in MetalLB + KafkaProxy on all 3
# clusters + kafkasql Apicurio). Run `make teardown && make mcs-setup && make e2e`
# from a clean shell for the full flow.
# Note: mcs-proxy-test is NOT included — Submariner Lighthouse only registers
# ClusterIP/Headless Services in clusterset.local DNS, so the LoadBalancer-type
# kafka-proxy Service (when externalAccess=LOADBALANCER is enabled) isn't
# resolvable as `kafka-proxy.kafka.svc.clusterset.local`. Cross-cluster reach is
# instead covered by `proxy-external-lb-test`, which has clients outside K8s
# hitting any cluster's MetalLB-assigned LB IP. Run `make mcs-proxy-test`
# manually only on a topology without externalAccess.
e2e: quorum kafkatopic-test rbac-test apicurio-rbac-test xml-filter-test \
     json-schema-test proxy-external-lb-test durability-test \
     rolling-restart-test broker-kill-test kafka-ui-setup ui-test
	@echo ""
	@echo "✓ All e2e checks passed"

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
