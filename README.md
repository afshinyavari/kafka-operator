# kafka-operator

A Kubernetes operator for running Apache Kafka 4.x in KRaft mode across multiple clusters. It manages controllers and brokers as separate node pools, handles safe rolling updates via ISR/quorum checks, and supports cross-cluster quorum via the Kubernetes Multi-Cluster Services (MCS) API — any MCS-compatible mesh works (Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster). The Kind reference setup uses Submariner.

## Features

- **Multi-cluster KRaft quorum** — single controller quorum spanning 3+ clusters via MCS `*.svc.clusterset.local` DNS (Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster, or any other MCS implementation)
- **Multi-cluster HA for KafkaProxy / ApicurioRegistry / KafkaUI** — `spec.mcs.enabled + spec.targetClusters` on each CR; the same CR is applied to every cluster and each operator filters by its own cluster ID. `ServiceExport` resources (the MCS-standard CRD) are auto-created for cross-cluster client resolution. For Apicurio, all replicas share one kafkasql journal topic on the MCS broker pool (single-partition for total ordering).
- **Node pool model** — separate `KafkaNodePool` CRs for controllers and brokers; mixed roles supported
- **Safe rolling updates** — spec hash triggers pod restart; ISR/quorum check blocks restart until safe
- **Cross-cluster roll ordering** — `spec.clusterRollOrder` sequences controller restarts across clusters
- **External access (NodePort / LoadBalancer / Gateway API / Ingress)** — per-broker NodePort services with deterministic port assignment; KafkaProxy / KafkaUI / Apicurio Schema Registry / Keycloak all expose the same four modes (KafkaProxy via TLS SNI passthrough, the HTTP services via HTTPRoute / standard Ingress with optional BYO TLS Secret). See [External access](#external-access) below.
- **TLS / mTLS listeners** — per-listener TLS with PKCS12 keystores generated at pod startup
- **Prometheus metrics across the data plane** — `spec.metricsConfig` opts in `jmx_prometheus_javaagent` + a `ServiceMonitor` on every Kafka broker pool, the Kroxylicious proxy (native `/metrics` on 9190), Cruise Control (JMX on 9101) and MirrorMaker2 (JMX on 9101). `ServiceMonitor` is applied via `OptionalResourceApplier` and silently no-ops on clusters without the Prometheus Operator CRD.
- **Pod Disruption Budget** — auto-created `maxUnavailable=1` per pool
- **Pod anti-affinity + topology spread** — brokers spread across nodes/zones
- **Storage per pool** — configurable PVC size and storage class
- **Resource requests/limits** — per-pool CPU/memory
- **Custom Kafka config** — cluster-wide and per-pool config maps merged at build time
- **Kafka version upgrades** — `spec.kafkaVersion` + `spec.targetMetadataVersion` with downgrade protection
- **Prometheus operator metrics** — rolling update counters, ISR check results, scale-down attempts
- **Kafka proxy (Kroxylicious)** — `KafkaProxy` CRD deploys Kroxylicious with custom filters; supports SASL/OAUTHBEARER client auth, JWT group-based RBAC, XML and schema-registry message validation
- **KafkaRbac** — declarative topic-level ACLs (group and user) and schema-registry artifact-level ACLs in a single CR; operator generates Kroxylicious and Apicurio policy ConfigMaps from it
- **Apicurio schema registry** — `ApicurioRegistry` CRD deploys Apicurio Registry with an optional HTTP RBAC proxy that enforces per-artifact READ/WRITE/DELETE access via JWT roles. Supports `mem`, `postgresql`, and `kafkasql` (durable Kafka-backed) storage with auto-provisioned journal topic and broker ACLs
- **OIDC/Keycloak integration** — JWT claims extracted from `realm_access.roles` become authorization groups; same JWT used for both Kafka proxy RBAC and schema registry proxy RBAC
- **MirrorMaker2 with Apicurio schema mirroring** — `MirrorMaker2` CRD deploys MM2 in dedicated mode. Each end (source/target) is independently a managed `KafkaCluster` ref or an external endpoint. When both ends carry Apicurio, an opt-in custom Connect SMT (`ApicurioSchemaTransferSmt`) rewrites the V3 envelope's globalId per record so consumers can decode mirrored payloads; it authenticates to a managed cluster's RBAC-proxied registry via OAuth2 client-credentials (`schemaRegistryAuthSecretRef`). `make -C kind mm2-mirror-test` is a full data + schema mirror e2e. See [api-reference.md#mirrormaker2](docs/api-reference.md#mirrormaker2).
- **Backup & restore** — `KafkaBackup` (scheduled), `KafkaRestore` (one-shot) and `KafkaBackupValidation` CRDs wrap the open-source [osodevops/kafka-backup](https://github.com/osodevops/kafka-backup) tool — compiled from source onto a UBI base. `KafkaBackup` builds a `CronJob` that copies topic records + consumer offsets (and, with `includeSchemas`, Apicurio schemas) to S3 / Azure / GCS / PVC; `KafkaRestore` builds an idempotent one-shot restore `Job` with point-in-time recovery and topic remapping. See [api-reference.md#kafkabackup](docs/api-reference.md#kafkabackup).
- **Cruise Control rebalancing** — `KafkaCluster.spec.cruiseControl` deploys [LinkedIn Cruise Control](https://github.com/linkedin/cruise-control) (compiled from source onto a UBI base) as a singleton on the primary cluster, adding the Cruise Control Metrics Reporter to every broker. The `KafkaRebalance` CRD is a declarative, Strimzi-style rebalance trigger: the operator generates an optimization proposal, the user approves it via the `kafka.yavari.afshin.se/rebalance=approve` annotation, and the operator executes it and tracks it to completion. Supports full / add-brokers / remove-brokers / disk-rebalance modes. See [api-reference.md#kafkarebalance](docs/api-reference.md#kafkarebalance).

## Prerequisites

- Java 21, Maven 3.9+
- Docker
- [`kind`](https://kind.sigs.k8s.io/) 0.23+
- [`subctl`](https://submariner.io/operations/deployment/subctl/) 0.17+ (only for the Submariner reference setup; if you bring your own MCS-compatible mesh, this isn't needed)
- `kubectl`

## Quick Start (Kind / Development)

```bash
# 1. Clone and build
git clone <repo> && cd kafka-operator

# 2. Spin up 3 Kind clusters with Submariner + deploy operator + CRs
make mcs-setup

# 3. Verify the KRaft quorum is healthy across all clusters
make quorum

# 4. Run the full e2e — KafkaTopic + RBAC + Apicurio + XML/JSON schema + MCS +
#    external LB + durability + rolling restart + broker-kill resilience + UI.
#    Each sub-target stays runnable individually (make rbac-test, etc.).
make -C kind e2e
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
├── schema-sync-smt/  # Apicurio-aware Kafka Connect SMT (shaded JAR; bundled into mm2:dev)
│   └── src/main/java/se/afshin/yavari/kafka/smt/
│       ├── ApicurioSchemaTransferSmt.java  # Envelope rewrite + reference DFS + LRU cache
│       └── ApicurioClient.java             # Minimal Apicurio v3 REST client
├── mm2-image/        # MirrorMaker2 worker image (kafka-ubi base + schema-sync-smt JAR)
├── kafka-backup-image/  # osodevops kafka-backup compiled from source on UBI + schema scripts
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

## External access

`KafkaProxy`, `KafkaUI`, and `ApicurioRegistry` each expose a `spec.externalAccess` field with the same four modes — `NODEPORT`, `LOADBALANCER`, `GATEWAY` (Gateway-API HTTPRoute / TLSRoute), and `INGRESS`. The HTTP services (UI, Apicurio rbac-proxy) accept an optional `tlsSecretRef` so the Ingress / Gateway listener can terminate TLS using a BYO Secret; otherwise the edge is plain HTTP. See [HTTP external access](docs/api-reference.md#http-external-access) for the full sub-spec.

Keycloak is deployed by a static manifest (`kind/manifests/keycloak.yaml`). `mcs-setup.sh` and `make keycloak-setup` honour two env vars:

- `KEYCLOAK_EXTERNAL=none|loadbalancer|ingress|gateway` (default `loadbalancer`).
- `KEYCLOAK_HOST=<dns>` (default `keycloak.kafka.svc.clusterset.local`) — used as the Ingress / HTTPRoute host.

The Deployment pins `KC_HOSTNAME=keycloak.kafka.svc.clusterset.local` so the JWT `iss` claim is identical regardless of how clients reach Keycloak. If a browser obtains tokens via a different hostname, the issuer mismatch fails token validation in `KafkaProxy` / `KafkaUI` / Apicurio. For dev/demo external access, keep the default `KEYCLOAK_HOST` and add an `/etc/hosts` entry on the laptop:

```
<external-LB-or-Ingress-IP>  keycloak.kafka.svc.clusterset.local
```

That keeps the issuer URL aligned for browser-side OAuth flows. Production deployments should instead set `KC_HOSTNAME` to a real external DNS name and configure all OIDC clients (proxy, UI, registry) to use that same name.

## Further Reading

- [Architecture](docs/architecture.md) — reconciler hierarchy, multi-cluster topology, rolling update flow
- [API Reference](docs/api-reference.md) — all CRD fields with types, defaults, and constraints
- [Operations Guide](docs/operations.md) — upgrades, scaling, monitoring, troubleshooting
