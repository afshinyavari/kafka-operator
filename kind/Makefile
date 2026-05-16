SHELL         := /bin/bash
export PATH   := $(HOME)/.local/bin:$(PATH)
CLUSTERS      := kafka-a kafka-b kafka-c
NAMESPACE     := kafka
IMAGE         := kafka-operator:dev
OPERATOR_DIR  := ..

.PHONY: all setup mcs-setup teardown status quorum logs-a logs-b logs-c build image help

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

reload-image:  ## Rebuild + reload image without recreating clusters
	$(MAKE) image
	@for cluster in $(CLUSTERS); do \
	  kind load docker-image $(IMAGE) --name $$cluster; \
	  kubectl --context kind-$$cluster -n $(NAMESPACE) rollout restart deployment kafka-operator; \
	done

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
