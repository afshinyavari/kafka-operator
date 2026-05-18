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
- **Kafka proxy (Kroxylicious)** — `KafkaProxy` CRD deploys Kroxylicious with custom filters; supports SASL/OAUTHBEARER client auth, JWT group-based RBAC, XML and schema-registry message validation
- **KafkaRbac** — declarative topic-level ACLs (group and user) and schema-registry artifact-level ACLs in a single CR; operator generates Kroxylicious and Apicurio policy ConfigMaps from it
- **Apicurio schema registry** — `ApicurioRegistry` CRD deploys Apicurio Registry with an optional HTTP RBAC proxy that enforces per-artifact READ/WRITE/DELETE access via JWT roles
- **OIDC/Keycloak integration** — JWT claims extracted from `realm_access.roles` become authorization groups; same JWT used for both Kafka proxy RBAC and schema registry proxy RBAC

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

### KafkaProxy with OIDC group-based RBAC

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaProxy
metadata:
  name: kafka-proxy
  namespace: kafka
spec:
  clusterRef: my-kafka
  poolRef: brokers-a
  replicas: 1
  image: kroxy-filters:dev
  clientPort: 9094
  rbacRef: kafka-rbac
  oidc:
    jwksEndpointUrl: http://keycloak.kafka.svc.cluster.local:8080/realms/demo/protocol/openid-connect/certs
    groupsClaim: realm_access.roles
```

### KafkaRbac with topic and schema-registry permissions

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRbac
metadata:
  name: kafka-rbac
  namespace: kafka
spec:
  groups:
    - name: orders-team
      kafka:
        topics: [orders, orders-dlq]
        operations: [PRODUCE, FETCH]
      schemaRegistry:
        artifacts: [orders]
        actions: [READ, WRITE]
    - name: schema-admin
      schemaRegistry:
        artifacts: ["*"]
        actions: [READ, WRITE, DELETE]
  users:
    - name: app1   # mTLS cert CN
      kafka:
        topics: [orders]
        operations: [PRODUCE]
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
| `make kroxy-image` | Build the custom Kroxylicious filter image (`kroxy-filters:dev`) |
| `make reload-kroxy-image` | Rebuild + reload filter image into all Kind clusters |
| `make apicurio-proxy-image` | Build the `apicurio-rbac-proxy` image |
| `make reload-apicurio-proxy-image` | Rebuild + reload proxy image into all Kind clusters |
| `make keycloak-setup` | Deploy Keycloak with realm `demo` to cluster-a, wait for Ready |
| `make proxy-setup` | Apply `KafkaRbac` + `KafkaProxy` CRs to cluster-a, wait for READY |
| `make rbac-test` | End-to-end SASL/OAUTHBEARER RBAC test (alice/bob × orders/invoices) |
| `make apicurio-setup` | Build + deploy `ApicurioRegistry` + RBAC proxy to cluster-a |
| `make apicurio-rbac-test` | End-to-end schema registry RBAC test (alice/bob × orders/invoices × READ/WRITE) |

## Project Layout

```
kafka-operator/
├── src/main/java/se/afshin/yavari/kafka/operator/
│   ├── crd/          # KafkaCluster, KafkaNodePool, KafkaPodSet, KafkaProxy, KafkaRbac, ApicurioRegistry
│   ├── reconciler/   # KafkaClusterReconciler, KafkaNodePoolReconciler, KafkaPodSetReconciler,
│   │                 # KafkaProxyReconciler, KafkaRbacReconciler, ApicurioRegistryReconciler
│   ├── proxy/        # KroxyliciousConfigBuilder, ProxyDeploymentBuilder, ProxyServiceBuilder,
│   │                 # KafkaRbacConfigMapBuilder, ApicurioDeploymentBuilder, …
│   ├── config/       # KRaftConfigGenerator, ServerPropertiesBuilder
│   ├── nodepool/     # PodTemplateFactory, StartupScriptBuilder, HeadlessServiceBuilder, …
│   ├── rolling/      # RollingUpdateController, IsrChecker, CrossClusterRollCoordinator
│   ├── upgrade/      # VersionUpgradeController, UpgradePhaseResource
│   └── metrics/      # OperatorMetrics (Micrometer/Prometheus)
├── src/main/resources/
│   └── application.properties
├── filters/          # Kroxylicious custom filter Maven project (kroxy-filters:dev image)
│   └── src/main/java/se/afshin/yavari/kroxy/
│       ├── auth/     # JwtGroupFilter, JwtGroupStore, GroupAwareAuthorizerService
│       └── auth/oauth/  # SaslHandshakeSynthesizerFilter, JwtGroupFilterFactory, …
├── apicurio-proxy/   # Apicurio RBAC proxy — standalone Quarkus service (apicurio-rbac-proxy:dev)
│   └── src/main/java/se/afshin/yavari/rbac/
│       ├── ProxyResource.java      # JAX-RS proxy with OIDC auth + RBAC check
│       └── PolicyEngine.java       # YAML policy loader with hot-reload via WatchService
├── kafka-image/      # UBI9-based Kafka wrapper image (Dockerfile + start.sh)
├── kind/
│   ├── mcs-setup.sh  # Full cluster bootstrap script
│   ├── Makefile      # All development targets
│   └── manifests/    # KafkaCluster, KafkaNodePool, KafkaProxy, KafkaRbac,
│                     # ApicurioRegistry, Keycloak YAML manifests
└── docs/
    ├── architecture.md   # Component design and data flow
    ├── api-reference.md  # CRD field reference
    └── operations.md     # Upgrades, scaling, proxy/RBAC setup, monitoring, troubleshooting
```

## Further Reading

- [Architecture](docs/architecture.md) — reconciler hierarchy, multi-cluster topology, rolling update flow
- [API Reference](docs/api-reference.md) — all CRD fields with types, defaults, and constraints
- [Operations Guide](docs/operations.md) — upgrades, scaling, monitoring, troubleshooting
