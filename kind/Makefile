SHELL              := /bin/bash
export PATH        := $(HOME)/.local/bin:$(PATH)
CLUSTERS           := kafka-a kafka-b kafka-c
NAMESPACE          := kafka
IMAGE              := kafka-operator:dev
KAFKA_IMAGE        := kafka-ubi:4.0.0
KAFKA_VERSION      := 4.0.0
KROXY_IMAGE        := kroxy-filters:dev
APICURIO_PROXY_IMG := apicurio-rbac-proxy:dev
OPERATOR_DIR       := ..
MANIFESTS_DIR      := manifests

.PHONY: all setup mcs-setup teardown status quorum proxy-test xml-filter-test \
        logs-a logs-b logs-c build image \
        kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
        keycloak-setup proxy-setup rbac-test apicurio-setup apicurio-rbac-test help

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

keycloak-setup:  ## Deploy Keycloak to cluster-a (namespace: kafka) and wait for Ready
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/keycloak.yaml
	@echo "Waiting for Keycloak to be Ready (up to 5 min)..."
	kubectl --context kind-kafka-a -n $(NAMESPACE) wait --for=condition=Ready \
	  pod -l app=keycloak --timeout=300s
	@echo "Keycloak ready."

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

apicurio-setup:  ## Build + deploy Apicurio Registry + RBAC proxy to cluster-a
	$(MAKE) reload-apicurio-proxy-image
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/kafkarbac.yaml
	kubectl --context kind-kafka-a apply -f $(MANIFESTS_DIR)/apicurioregistry.yaml
	@echo "Waiting for ApicurioRegistry to be Ready..."
	@until kubectl --context kind-kafka-a -n $(NAMESPACE) get apicurioregistry apicurio \
	    -o jsonpath='{.status.phase}' 2>/dev/null | grep -q READY; do sleep 5; done
	@echo "ApicurioRegistry READY."

apicurio-rbac-test:  ## Run end-to-end schema registry RBAC test
	@chmod +x apicurio-rbac-test.sh && ./apicurio-rbac-test.sh

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
