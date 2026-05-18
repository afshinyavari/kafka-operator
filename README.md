# kafka-operator

A Kubernetes operator for running Apache Kafka 4.x in KRaft mode across multiple clusters. It manages controllers and brokers as separate node pools, handles safe rolling updates via ISR/quorum checks, and supports cross-cluster quorum via Submariner MCS.

## Features

- **Multi-cluster KRaft quorum** — single controller quorum spanning 3+ clusters via Submariner `clusterset.local` DNS
- **Node pool model** — separate `KafkaNodePool` CRs for controllers and brokers; mixed roles supported
- **Safe rolling updates** — spec hash triggers pod restart; ISR/quorum check blocks restart until safe
- **Cross-cluster roll ordering** — `spec.clusterRollOrder` sequences controller restarts across clusters
- **External access (NodePort)** — per-broker NodePort services with deterministic port assignment
- **TLS / mTLS listeners** — per-listener TLS with PKCS12 keystores generated at pod startup
- **JMX metrics** — `jmx_prometheus_javaagent` with optional Prometheus `ServiceMonitor`
- **Pod Disruption Budget** — auto-created `maxUnavailable=1` per pool
- **Pod anti-affinity + topology spread** — brokers spread across nodes/zones
- **Storage per pool** — configurable PVC size and storage class
- **Resource requests/limits** — per-pool CPU/memory
- **Custom Kafka config** — cluster-wide and per-pool config maps merged at build time
- **Kafka version upgrades** — `spec.kafkaVersion` + `spec.targetMetadataVersion` with downgrade protection
- **Prometheus operator metrics** — rolling update counters, ISR check results, scale-down attempts

## Prerequisites

- Java 21, Maven 3.9+
- Docker
- [`kind`](https://kind.sigs.k8s.io/) 0.23+
- [`subctl`](https://submariner.io/operations/deployment/subctl/) 0.17+ (for MCS mode)
- `kubectl`

## Quick Start (Kind / Development)

```bash
# 1. Clone and build
git clone <repo> && cd kafka-operator

# 2. Spin up 3 Kind clusters with Submariner + deploy operator + CRs
make mcs-setup

# 3. Verify the KRaft quorum is healthy across all clusters
make quorum
```

The setup deploys:
- 3 Kind clusters: `kafka-a`, `kafka-b`, `kafka-c`
- One controller pod and one broker pod per cluster
- Single KRaft quorum spanning all three clusters

## CR Examples

### Minimal KafkaCluster

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaCluster
metadata:
  name: my-kafka
  namespace: kafka
  labels:
    kafka.yavari.afshin.se/cluster: my-kafka
spec:
  kafkaImage: kafka-ubi:4.0.0
  kafkaVersion: "4.0"
  clusters:
    - id: A
      controllerAdvertisedAddress: controllers-a-headless.kafka.svc.clusterset.local:9093
      operatorAddress: kafka-operator-a.kafka.svc.clusterset.local:8080
    - id: B
      controllerAdvertisedAddress: controllers-b-headless.kafka.svc.clusterset.local:9093
      operatorAddress: kafka-operator-b.kafka.svc.clusterset.local:8080
    - id: C
      controllerAdvertisedAddress: controllers-c-headless.kafka.svc.clusterset.local:9093
      operatorAddress: kafka-operator-c.kafka.svc.clusterset.local:8080
  clusterRollOrder: ["A", "B", "C"]
```

### Broker Pool with External NodePort Access

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers-a
  namespace: kafka
  labels:
    kafka.yavari.afshin.se/cluster: my-kafka
spec:
  roles: [BROKER]
  replicas: 3
  storage:
    size: 50Gi
    storageClassName: standard
  resources:
    requests:
      cpu: "500m"
      memory: "2Gi"
    limits:
      memory: "2Gi"
```

To add a NodePort external listener, patch the `KafkaCluster`:

```yaml
spec:
  listeners:
    - name: EXTERNAL
      port: 9094
      externalAccess: NODEPORT
      nodePortBase: 31000   # broker-0 → 31000, broker-1 → 31001, …
```

## Make Targets

| Target | Description |
|--------|-------------|
| `make mcs-setup` | Full setup: 3 Kind clusters + Flannel + Submariner + operator + CRs |
| `make reload-image` | Rebuild operator jar + Docker image + hot-swap into running clusters (~30s) |
| `make quorum` | Print KRaft quorum state across all clusters |
| `make status` | Show pod/CR status across all clusters |
| `make teardown` | Destroy all Kind clusters |
| `make logs-a` / `logs-b` / `logs-c` | Tail operator logs on cluster A/B/C |
| `FORCE_BUILD=1 make mcs-setup` | Force rebuild of Docker images even if they already exist |

## Project Layout

```
kafka-operator/
├── src/main/java/se/afshin/yavari/kafka/operator/
│   ├── crd/          # KafkaCluster, KafkaNodePool, KafkaPodSet CRD classes
│   ├── reconciler/   # KafkaClusterReconciler, KafkaNodePoolReconciler, KafkaPodSetReconciler
│   ├── config/       # KRaftConfigGenerator, ServerPropertiesBuilder
│   ├── nodepool/     # PodTemplateFactory, StartupScriptBuilder, HeadlessServiceBuilder, …
│   ├── rolling/      # RollingUpdateController, IsrChecker, CrossClusterRollCoordinator
│   ├── upgrade/      # VersionUpgradeController, UpgradePhaseResource
│   └── metrics/      # OperatorMetrics (Micrometer/Prometheus)
├── src/main/resources/
│   └── application.properties
├── kind/
│   ├── mcs-setup.sh  # Full cluster bootstrap script
│   ├── Makefile      # All development targets
│   └── manifests/    # KafkaCluster + KafkaNodePool YAML examples
├── kafka-image/      # UBI9-based Kafka wrapper image (Dockerfile + start.sh)
└── docs/
    ├── architecture.md   # Component design and data flow
    ├── api-reference.md  # CRD field reference
    └── operations.md     # Upgrades, scaling, monitoring, troubleshooting
```

## Further Reading

- [Architecture](docs/architecture.md) — reconciler hierarchy, multi-cluster topology, rolling update flow
- [API Reference](docs/api-reference.md) — all CRD fields with types, defaults, and constraints
- [Operations Guide](docs/operations.md) — upgrades, scaling, monitoring, troubleshooting
