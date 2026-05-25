# Architecture

## Overview

The kafka-operator is a Kubernetes operator built with [Java Operator SDK (JOSDK)](https://javaoperatorsdk.io/) and [Quarkus](https://quarkus.io/). It manages Apache Kafka 4.x clusters running in KRaft mode (no ZooKeeper) across multiple Kubernetes clusters connected by any [Multi-Cluster Services (MCS)](https://github.com/kubernetes/enhancements/tree/master/keps/sig-multicluster/1645-multi-cluster-services-api) implementation — [Submariner Lighthouse](https://submariner.io/), [Cilium Cluster Mesh](https://docs.cilium.io/en/stable/network/clustermesh/), [Istio multi-cluster](https://istio.io/latest/docs/setup/install/multicluster/), or any other compliant mesh. The Kind reference setup uses Submariner.

Three reconcilers form a strict hierarchy. Pods are never created directly — they are described in an operator-managed `KafkaPodSet` CRD, which gives the operator full control over the pod lifecycle without using StatefulSets.

---

## CRD Hierarchy

### Kafka cluster tree

```
KafkaCluster  (user-managed)
  │  Produces: quorum ConfigMap
  │
  └── KafkaNodePool  (user-managed, 1..N per cluster)
        │  Produces: pool ConfigMap, headless Service, PDB,
        │            external Services, ServiceExport (MCS), KafkaPodSet
        │
        └── KafkaPodSet  (operator-managed, 1 per pool)
              │  Produces: Pods, PVCs
              │
              └── Pod  (operator-managed, 1 per replica)
```

### Proxy / RBAC tree (independent, optional)

```
KafkaRbac  (user-managed)
  │  Produces:
  │    {name}-kafka-rules ConfigMap    → mounted by KafkaProxy
  │    {name}-apicurio-policy ConfigMap → mounted by apicurio-rbac-proxy
  │
  ├── KafkaProxy  (user-managed, references KafkaNodePool via poolRef)
  │     Produces: {name}-config ConfigMap (Kroxylicious YAML),
  │               Deployment (Kroxylicious + custom filters), Service
  │
  └── ApicurioRegistry  (user-managed, references KafkaRbac via rbacRef)
        Produces: {name}-registry Deployment + Service,
                  {name}-rbac-proxy Deployment + Service
```

Users create `KafkaRbac`, `KafkaProxy`, `ApicurioRegistry`, and `KafkaTopic`. The operator creates and owns all downstream resources.

`KafkaTopic` is structurally different from the other CRs — it doesn't produce Kubernetes objects but instead reconciles a single shared Kafka object (a topic in the cluster metadata) against an AdminClient. See [KafkaTopic — primary-cluster model](#kafkatopic--primary-cluster-model) below.

---

## Reconciler Responsibilities

| Reconciler | Watches | Creates / Manages | Triggered by |
|------------|---------|-------------------|--------------|
| `KafkaClusterReconciler` | `KafkaCluster` | `{cluster}-quorum-config` ConfigMap; deletion ordering | `KafkaCluster` changes; `KafkaPodSet` status changes |
| `KafkaNodePoolReconciler` | `KafkaNodePool` | Pool ConfigMap, headless Service, per-broker external Services, PDB, ServiceExport, KafkaPodSet | `KafkaNodePool` changes; parent `KafkaCluster` changes |
| `KafkaPodSetReconciler` | `KafkaPodSet` | Pods, PVCs; rolling updates | `KafkaPodSet` changes; Pod changes |
| `KafkaRbacReconciler` | `KafkaRbac` | `{name}-kafka-rules` ConfigMap, `{name}-apicurio-policy` ConfigMap | `KafkaRbac` changes |
| `KafkaProxyReconciler` | `KafkaProxy` | `{name}-config` ConfigMap (Kroxylicious YAML), Deployment, Service | `KafkaProxy`, `KafkaNodePool`, `KafkaRbac`, `ApicurioRegistry` changes |
| `ApicurioRegistryReconciler` | `ApicurioRegistry` | `{name}-registry` Deployment + Service, `{name}-rbac-proxy` Deployment + Service | `ApicurioRegistry`, `KafkaRbac` changes |
| `KafkaTopicReconciler` | `KafkaTopic` | Kafka topics via `AdminClient` (createTopics, incrementalAlterConfigs, createPartitions, deleteTopics) | `KafkaTopic` changes; periodic resync every 5 min for external-drift detection |
| `KafkaBackupReconciler` | `KafkaBackup` | `{name}` ConfigMap (rendered config) + `CronJob` running the kafka-backup tool | `KafkaBackup` changes; 60s resync for run-result status |
| `KafkaRestoreReconciler` | `KafkaRestore` | `{name}` ConfigMap + one-shot restore `Job` (created once; idempotent) | `KafkaRestore` changes |
| `KafkaBackupValidationReconciler` | `KafkaBackupValidation` | one-shot validation `Job` | `KafkaBackupValidation` changes |
| `KafkaRebalanceReconciler` | `KafkaRebalance` | Drives Cruise Control via its REST API (proposal → approve → execute) | `KafkaRebalance` changes; poll while a Cruise Control task runs |

All reconcilers use JOSDK's `UpdateControl.patchStatus().rescheduleAfter(15s)` when work is still in progress, creating a self-healing loop.

Cruise Control itself is not a top-level reconciler — it is an optional sub-component of `KafkaCluster`. When `spec.cruiseControl` is set, `KafkaClusterReconciler` delegates to `CruiseControlOrchestrator` (mirroring the proxy/Apicurio orchestrators), which deploys one Cruise Control Deployment + Service on the primary cluster only.

---

## Multi-Cluster KRaft Topology

```
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
│      kafka-a        │  │      kafka-b        │  │      kafka-c        │
│                     │  │                     │  │                     │
│  controllers-a-0    │  │  controllers-b-0    │  │  controllers-c-0    │
│  (nodeId: 10000)    │  │  (nodeId: 10001)    │  │  (nodeId: 10002)    │
│        │            │  │        │            │  │        │            │
│  ──────┼────────────┼──┼────────┼────────────┼──┼────────┼───────     │
│   KRaft quorum via controllers-*.kafka.svc.clusterset.local:9093       │
│  ──────┼────────────┼──┼────────┼────────────┼──┼────────┼───────     │
│        │            │  │        │            │  │        │            │
│  brokers-a-0        │  │  brokers-b-0        │  │  brokers-c-0        │
│  (nodeId: 0)        │  │  (nodeId: 1000)     │  │  (nodeId: 2000)     │
│                     │  │                     │  │                     │
└─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

- Controllers form a **single KRaft quorum** across all clusters. Each controller advertises its address via `*.svc.clusterset.local` DNS provided by the cluster fabric's MCS implementation (Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster, etc.).
- Brokers are **independent per cluster** but belong to the same Kafka cluster (same `cluster.id`). Cross-cluster topic replication is handled at the application level, not by the operator.
- Node IDs are deterministic: controller at cluster index `i` gets `10000 + i`; broker at cluster `i` ordinal `j` gets `i * 1000 + j`. This allows up to 10 clusters × 1000 brokers each before collision.

### MCS vs Non-MCS Mode

Set `KAFKA_NETWORKING_MCS_ENABLED=true` on the operator deployment to switch modes:

| | MCS mode | Non-MCS mode |
|-|----------|-------------|
| Broker `INTERNAL` advertised address | `{pool}-headless.kafka.svc.clusterset.local` (shared) | `{pod}.{pool}-headless.kafka.svc.cluster.local` (per-pod) |
| Cross-cluster controller reach | `svc.clusterset.local` DNS | Must configure manually |
| `ServiceExport` resources | Created automatically | Not created |

### Multi-cluster HA for stateless services (KafkaProxy / ApicurioRegistry / KafkaUI)

`KafkaProxy`, `ApicurioRegistry`, and `KafkaUI` all opt into multi-cluster HA via the same shape:

```yaml
spec:
  mcs:
    enabled: true
  targetClusters: [A, B, C]
```

The same CR is applied to every K8s cluster; each operator filters by its own `KAFKA_CLUSTER_ID` env var. Operators on clusters listed in `targetClusters` reconcile fully; operators on other clusters set `status.phase=SKIPPED` with no resources created. When `mcs.enabled=true` the operator also creates an MCS `ServiceExport` (the standard MCS-spec CRD) so cross-cluster clients can resolve the service via `<svc>.<ns>.svc.clusterset.local` — **but only for ClusterIP or Headless underlying Services**, per the MCS spec. Submariner Lighthouse explicitly rejects `LoadBalancer`-typed Services (`UnsupportedServiceType`); other MCS implementations have the same restriction. With `externalAccess.type=LOADBALANCER` the export is created but never aggregated, and clients reach each cluster via its own LB IP instead. Use `GATEWAY` or `INGRESS` modes if you need cross-cluster DNS aggregation.

For Apicurio specifically: all replicas across all clusters share **one** kafkasql journal topic on the MCS broker pool. Apicurio v2.6 requires the journal to have `partitions=1` for total ordering — the operator warns if the override is > 1. The kafkasql client cert (`schema-registry-client-tls` Secret with `CN=apicurio-registry`) is distributed to every cluster by `mcs-setup.sh`, so all replicas authenticate as the same Kafka principal and share ACLs. Concurrent writes to the same artifact-version across clusters are resolved by Apicurio's optimistic concurrency (one client gets a `409 Conflict`); this is rare and safe.

For Kafka UI: state is read-mostly (per-pod Caffeine cache, OIDC session local to each pod). External clients pin to one cluster's LB IP / Ingress host; for cross-cluster fallback, internal callers can resolve `kafka-ui.kafka.svc.clusterset.local` via the MCS mesh's DNS.

---

## Config Generation Pipeline

Each node pool's Kafka configuration is built at reconcile time and stored in a ConfigMap, then mounted into the pod as a template. Final substitution happens at pod startup.

```
KafkaNodePoolReconciler
  │
  └── PoolConfigMapBuilder
        ├── ServerPropertiesBuilder.buildProperties(...)
        │     Merges: cluster spec.config
        │           → pool spec.config (overrides)
        │           → computed fields (node.id, listeners, quorum voters, …)
        │     Output: server.properties.template  (with ${NODE_ID}, ${ADVERTISED_ADDR}, etc.)
        │
        └── StartupScriptBuilder.build(...)
              Output: start.sh
              At pod boot:
                1. Compute NODE_ID from POD_NAME ordinal
                2. Compute ADVERTISED_ADDR (MCS or per-pod FQDN)
                3. Compute per-listener addr vars (internal: from ADVERTISED_ADDR; NodePort: from HOST_IP)
                4. sed-substitute all vars → /tmp/server.properties
                5. kafka-storage.sh format (idempotent)
                6. (optional) Convert TLS certs to PKCS12 keystores
                7. exec kafka-server-start.sh /tmp/server.properties
```

The template approach means the ConfigMap is cluster-wide (same for all replicas in a pool), while runtime values like `NODE_ID` and `HOST_IP` are injected per-pod via environment variables.

---

## Rolling Update Flow

A rolling update is triggered whenever a pod's current spec hash differs from the desired hash stored in the `KafkaPodSet`.

```
Spec change detected (kafkaImage, config, version, etc.)
  │
  ▼
KafkaNodePoolReconciler recomputes PodSpec → new hash → updates KafkaPodSet
  │
  ▼
KafkaPodSetReconciler sees hash mismatch on pod P
  │
  ├── CrossClusterRollCoordinator.isMyTurnToRoll()?   ← brokers AND controllers
  │     Polls GET /operator/upgrade-phase on preceding clusters in clusterRollOrder.
  │     Blocks until all predecessors report upgradePhase=IDLE.
  │     RollTracker.markRolling("podset", …) flips this cluster to ROLLING for the
  │     duration of the roll, so successor clusters see ROLLING even on the
  │     success path (etcd never observes status.currentRollingPod=non-empty
  │     during a successful roll).
  │
  ▼
RollingUpdateController.rollPod(P)
  ├── IsrChecker.isBrokerSafeToRestart()   ← AdminClient: no sole-ISR partition
  ├── IsrChecker.isControllerSafeToRestart() ← AdminClient: quorum ≥2 voters, no lagged peers
  ├── Delete pod P
  ├── Wait: pod disappears from API
  ├── Create pod P (new spec)
  └── Wait: pod reaches Ready condition
  │
  ▼
KafkaPodSetReconciler updates status.currentRollingPod → ""
Next pod in the same pool may now roll (one at a time)
```

If the ISR/quorum check fails, the reconciler reschedules after 15 seconds and retries — the update is non-blocking for other pools.

---

## Cert Rotation

The operator does not own or sign TLS material — cert-manager (or whoever) provisions the Kubernetes `Secret`s and rotates them on its own schedule. The operator's job is to roll the affected workloads when those Secrets change, so the running pods stop serving the old in-memory cert.

Two pieces wire this up:

1. **Secret revisions are folded into `configHash`.** Each reconciler computes `configHash` over the rendered config + the `metadata.resourceVersion` of every TLS `Secret` the pod mounts, via `infra/SecretRevisionTracker`. When cert-manager writes a new cert, the resourceVersion bumps, the hash flips, the PodTemplate annotation changes, and Kubernetes rolls the workload.

2. **A `Secret` informer wakes the reconciler.** Each reconciler registers `InformerEventSource<Secret>` with a `secondaryToPrimaryMapper` that returns the CRs whose Secret refs match the changed Secret's name. Without this, a rotation would sit unreconciled until the next periodic resync.

```
cert-manager rotates Secret S
  │
  ▼
Informer fires → mapper returns matching CRs → reconciler wakes
  │
  ▼
revisionsOf(...) reads S.metadata.resourceVersion (new value)
configHash = SHA256(config || revisions) → flips
  │
  ▼
PodTemplate annotation changes → standard rolling-update flow above
```

Per-component Secret coverage:

| Component                  | Tracked Secrets                                                                          |
|----------------------------|------------------------------------------------------------------------------------------|
| `KafkaNodePool` (brokers)  | `{poolName}-broker-tls` (mTLS), `{podName}-tls` per replica (controller + listener TLS) |
| `KafkaProxy`               | `clientCertSecretRef` (default `{name}-client-tls`), `serverCertSecretRef` (default `{name}-server-tls`) |
| `ApicurioRegistry`         | `storage.tlsSecretRef` (kafkasql client cert)                                            |

The blast radius of a rotation is just the pool / Deployment whose Secret changed. Cross-cluster ordering follows `KafkaCluster.spec.clusterRollOrder` for every cluster-spanning workload (controllers, brokers, proxy, Apicurio), so a synchronized rotation across all three MCS clusters rolls strictly one cluster at a time. The `RollTracker` in-memory marker closes the visibility window where a successful roll wouldn't otherwise surface in etcd (status is patched only after the multi-minute `rollPod()` returns).

The `configHash` itself is a 12-char truncated SHA-256 emitted on the PodTemplate annotation `kafka.yavari.afshin.se/config-hash`. Two trade-offs worth knowing:

- **Watching all Secrets in the namespace.** The informer has no label filter (cert-manager doesn't set one we own), so the operator caches every Secret in the watched namespace. This is fine at typical scale; revisit if the namespace holds thousands of Secrets.
- **No dynamic reload.** Kafka 2.4+ supports in-place TLS reload via `incrementalAlterConfigs`, but the operator restarts pods instead. This keeps the rotation path consistent with all other config changes and avoids broker-state edge cases.

---

## External Access (NodePort)

When a listener has `externalAccess: NODEPORT`, the operator creates one NodePort `Service` per broker pod, with a deterministic `nodePort = nodePortBase + podOrdinal`.

```
KafkaNodePoolReconciler
  └── ExternalAccessServiceBuilder.applyExternalServices(...)
        For ordinal N, listener L:
          Service name: {pool}-{N}-{listener}-ext
          selector: { kafka.node.id: "{nodeId}", kafka-node-pool: "{pool}" }
          nodePort: L.nodePortBase + N

PodTemplateFactory.build(...)
  For each pod at ordinal N:
    env HOST_IP        = status.hostIP   (Downward API)
    env EXTERNAL_{L}_NODEPORT = nodePortBase + N  (literal)

StartupScriptBuilder (in start.sh):
  {L}_ADDR="${HOST_IP}:${EXTERNAL_{L}_NODEPORT}"

ServerPropertiesBuilder:
  advertised.listeners = INTERNAL://...,{L}://${L}_ADDR
```

Each broker therefore advertises its own node's IP and its dedicated port. Clients connecting to `<nodeIP>:<nodePort>` reach exactly that broker's pod via kube-proxy.

---

## Upgrade / Metadata Version Flow

Kafka 4.x uses a `metadata.version` feature flag to gate new wire protocol features. The operator manages version upgrades in phases:

```
spec.kafkaVersion changed  →  upgradePhase = ROLLING
  All pods rolling to new image
  upgradePhase = IDLE (when all pods ready)

spec.targetMetadataVersion set  →  VersionUpgradeController
  AdminClient.updateFeatures({ "metadata.version" → targetMetadataVersion })
  status.currentMetadataVersion updated
```

Downgrade protection: `CrValidator` rejects a `targetMetadataVersion` lower than `status.currentMetadataVersion`, and rejects a `kafkaVersion` lower than `status.currentKafkaVersion`.

---

## Key Classes Quick Reference

| Class | Package | Role |
|-------|---------|------|
| `KRaftConfigGenerator` | `config` | Node ID arithmetic, quorum voters string, deterministic cluster ID |
| `ServerPropertiesBuilder` | `config` | Merges cluster + pool + computed Kafka properties |
| `StartupScriptBuilder` | `nodepool` | Generates `start.sh` with runtime template substitution |
| `PodTemplateFactory` | `nodepool` | Builds full `PodSpec` with env vars, volumes, affinity, probes |
| `ExternalAccessServiceBuilder` | `nodepool` | Per-broker NodePort/LB service lifecycle |
| `HeadlessServiceBuilder` | `nodepool` | Headless `ClusterIP: None` service for intra-cluster DNS |
| `PvcFactory` | `podset` | Idempotent PVC creation from `StorageSpec` |
| `PodSpecHasher` | `podset` | JSON-serialises `PodSpec` → SHA-256 hex for change detection |
| `RollingUpdateController` | `rolling` | Orchestrates delete → wait-gone → create → wait-ready |
| `IsrChecker` | `rolling` | AdminClient queries for ISR and KRaft quorum safety |
| `CrossClusterRollCoordinator` | `rolling` | HTTP polling of peer operators' `/operator/upgrade-phase` |
| `VersionUpgradeController` | `upgrade` | Bumps `metadata.version` via AdminClient after image upgrade |
| `ClusterStatusAggregator` | `cluster` | Rolls up pool readiness into `KafkaCluster.status` |
| `CrValidator` | `reconciler` | Semantic validation beyond CRD schema constraints |
| `OperatorMetrics` | `metrics` | Micrometer counters/timers for rolling updates, ISR, scale-down |
| `KroxyliciousConfigBuilder` | `proxy` | Generates Kroxylicious `config.yaml` from `KafkaProxy` spec |
| `KafkaRbacConfigMapBuilder` | `proxy` | Generates `rbac-rules.yaml` and `policy.yaml` from `KafkaRbac` spec |
| `GroupAwareAuthorizerService` | `filters/…/rbac` | Kroxylicious `AuthorizerService` plugin; enforces topic RBAC |
| `PolicyEngine` | `apicurio-proxy/…/rbac` | YAML policy loader with `WatchService` hot-reload; enforces artifact RBAC |
| `KafkaTopicService` | `topic` | AdminClient ops + pure diff logic (`computeConfigDiff`, `computePartitionAction`) |
| `BrokerBootstrapResolver` | `topic` | Picks the alphabetically-first broker `KafkaNodePool` and builds its headless bootstrap address |
| `TopicReconcileLeader` / `StaticPrimaryClusterLeader` | `topic` | Cross-cluster single-writer gate. v1 impl returns `localClusterId == spec.clusters[0].id`; v2 will swap in a Kafka consumer-group leader implementation |
| `AdminClientTlsLoader` | `topic` | Reads a cert-manager TLS Secret (PEM) and returns Kafka client SSL properties using `ssl.keystore.type=PEM` (no PKCS12 conversion) |
| `MetricsResources` | `infra` | Shared builder for the `<name>-metrics` ClusterIP Service + `monitoring.coreos.com/v1` ServiceMonitor used by every workload that exposes Prometheus metrics (node pools, proxy, Cruise Control, MM2). Also loads bundled JMX exporter configs from the classpath. |
| `OptionalResourceApplier` | `infra` | Apply/delete wrapper for ServiceMonitor / TLSRoute / HTTPRoute / Ingress that silently no-ops when the CRD is absent on the cluster. |

---

## KafkaTopic — primary-cluster model

The `KafkaTopic` CRD reconciles a *shared* object (a Kafka topic in the cluster's metadata), not a per-K8s-cluster Kubernetes resource. In MCS deployments, three operator instances all watch the same CR; we must elect a single writer to avoid AdminClient races.

**v1 model: static primary.** The operator instance whose `KAFKA_CLUSTER_ID` matches `spec.clusters[0].id` on the referenced `KafkaCluster` is the leader. Peer operators look up the CR, mark `status.phase=SKIPPED`, and do nothing else. This means:

- The CR can be deployed via GitOps to every K8s cluster identically — only one operator acts on it.
- Status in non-primary clusters always shows `SKIPPED`. The authoritative `READY` only appears in the primary cluster's view of the CR. This is a known v1 trait.
- Failover is manual: if the primary cluster is down, reorder `spec.clusters[]` on the `KafkaCluster` CR.

The leadership check sits behind `TopicReconcileLeader`; a future `KafkaConsumerGroupLeader` impl will use a 1-partition coordination topic + consumer-group rebalance for dynamic failover, with passive operators consuming the leader's reconciled-status messages to fan status out to all K8s clusters. CRD surface unchanged.

**Bootstrap resolution.** `BrokerBootstrapResolver` picks the alphabetically-first `KafkaNodePool` labeled `kafka.yavari.afshin.se/cluster=<clusterRef>` with role `BROKER`, and addresses it as `<pool>-headless.<ns>.svc.cluster.local:9092`. AdminClient only needs one reachable broker — Kafka metadata discovery handles the rest.

**Reconcile flow** (leader only): describe → create-if-missing OR (RF-mismatch → FAIL | partition-decrease → FAIL | partition-increase → expand | config-diff → incremental-alter) → re-describe → patch status. Reschedule every 5 min for external-drift detection.

**Cleanup.** `Cleaner` honours `spec.deletionPolicy`: `DELETE` runs `deleteTopics` (swallows `UnknownTopicOrPartitionException`); `RETAIN` just releases the finalizer. If Kafka is unreachable during a `DELETE` cleanup, the finalizer is held and the operation is retried — we won't let the CR finalize while leaving the topic up.

**AdminClient transport:** When `KafkaCluster.spec.proxyMtls.enabled=false`, the AdminClient connects plaintext. When `proxyMtls.enabled=true`, the operator reads a cert-manager-style PEM Secret (default name `kafka-operator-client-tls`, overridable via `spec.proxyMtls.adminClientCertSecretRef`) and passes `tls.crt`/`tls.key`/`ca.crt` directly to the Kafka client via PEM source mode (no PKCS12 conversion). The cert's CN must be in broker `super.users` — by convention reuse the existing `proxyPrincipal` so brokers don't need a config change. `IsrChecker` still uses the plaintext path (its "treat unreachable as safe" behaviour means rolling updates aren't blocked by mTLS); migrating it to the same loader is straightforward future work.

---

## Cruise Control & KafkaRebalance

Cruise Control automates partition rebalancing. It has two halves:

**Deployment (`KafkaCluster.spec.cruiseControl`).** An optional sub-component, like the proxy and Apicurio. `KafkaClusterReconciler` delegates to `CruiseControlOrchestrator`, which:

- Deploys Cruise Control as a **singleton** — one Deployment + ClusterIP Service on the primary cluster (`spec.clusters[0]`) only; other clusters report `SKIPPED`. Cruise Control reaches brokers in every MCS cluster via their advertised `INTERNAL` listeners.
- Renders `cruisecontrol.properties` (`CruiseControlConfigBuilder`) and `capacity.json` (`CruiseControlCapacityBuilder`) into a ConfigMap. Cruise Control runs in KRaft mode (`kafka.broker.failure.detection.enable=true`, no ZooKeeper).
- Adds the **Cruise Control Metrics Reporter** to every broker. `ServerPropertiesBuilder` appends `metric.reporters` (and, under mTLS, the reporter's SSL config) when `spec.cruiseControl` is set — a one-time broker roll via the standard config-hash mechanism. The reporter JAR is compiled from source into the Kafka image; the Cruise Control server image (`cruise-control-image/`) is likewise compiled from source onto a UBI base.
- Under broker mTLS, Cruise Control's AdminClient reuses the operator's admin client cert (PEM → PKCS12 via the shared `PemToPkcs12InitContainer`).

**Rebalancing (`KafkaRebalance` CRD).** `KafkaRebalanceReconciler` is thin; all transitions live in `RebalanceStateMachine`, which drives Cruise Control over its REST API via the mockable `CruiseControlClient` seam (`HttpCruiseControlClient` is the production impl). State machine:

```
NEW → PENDING_PROPOSAL → PROPOSAL_READY ──(annotate approve)──> REBALANCING → READY
                              │                                      │
                              └──(refresh → NEW) (stop → STOPPED)─────┘
```

Cruise Control's REST API is asynchronous — a long request returns a `User-Task-ID`; the operator persists it (`status.sessionId` / `status.executionTaskId`) and re-issues the identical request to fetch the cached result. Approval is annotation-driven (`kafka.yavari.afshin.se/rebalance=approve|refresh|stop`); the operator consumes and clears the annotation. Like `KafkaTopic`, the reconciler only acts from the primary cluster.

---

## Proxy Layer (KafkaProxy)

`KafkaProxy` deploys [Kroxylicious](https://kroxylicious.io/) as a transparent Kafka proxy.
Custom filters are compiled into the `kroxy-filters:dev` image (Maven project under `filters/`).

### Filter chain — OIDC mode

When `spec.oidc` is set, `KroxyliciousConfigBuilder` generates a four-filter chain. Filters
run **in order on the request path** and **in reverse on the response path**:

```
Request path (client → broker):
  jwt-groups → oauth-bearer-validation → sasl-handshake-synthesizer → [authorization]

Response path (broker → client, reversed):
  [authorization] → sasl-handshake-synthesizer → oauth-bearer-validation → jwt-groups
```

**Why this order matters:**

The backend Kafka broker runs on a PLAINTEXT listener with no SASL support. When
`OauthBearerValidationFilter` forwards `SASL_AUTHENTICATE` to the broker, the broker
returns error 34 (`ILLEGAL_SASL_STATE`). On the response path, `SaslHandshakeSynthesizerFilter`
must run **first** — it converts that error into a success response, so that when
`OauthBearerValidationFilter` processes the response it calls `clientSaslAuthenticationSuccess()`
with the validated JWT subject. Incorrect order leaves Subject as anonymous, and all RBAC
decisions default to DENY.

### JwtGroupStore side-channel

`OauthBearerValidationFilter` builds `Subject{User(sub_UUID)}` internally — it does not
call any externally registered `SaslSubjectBuilderService` SPI. This means groups are
**not** in the Subject's principal set. To bridge the gap:

1. `JwtGroupFilter` parses the JWT on the `SASL_AUTHENTICATE_REQUEST` path and stores
   `sub → Set<groupName>` in the static `JwtGroupStore`.
2. `GroupAwareAuthorizer.isAllowed()` checks `subject.allPrincipalsOfType(Group.class)`;
   if empty, it falls back to `JwtGroupStore.get(user.name())` using the same sub UUID.

### Operations and implicit `DESCRIBE`

`KafkaProxy` RBAC rules use Kafka's native ACL operation names:
`READ`, `WRITE`, `DESCRIBE`, `CREATE`, `DELETE`, `ALTER`,
`DESCRIBE_CONFIGS`, `ALTER_CONFIGS` (plus `*` for everything).

Mirroring Apache Kafka's `StandardAuthorizer`, `GroupAwareAuthorizer.matchesOp()`
treats some grants as implying `DESCRIBE`:

| Granting…                                 | Also grants…        |
|-------------------------------------------|---------------------|
| `READ`, `WRITE`, `DELETE`, `ALTER`        | `DESCRIBE`          |
| `ALTER_CONFIGS`                           | `DESCRIBE_CONFIGS`  |

This matters because a Kafka producer first sends a `METADATA` request, which
Kroxylicious authorises as `DESCRIBE`. Without the implicit rule, a role that
lists only `WRITE` would have METADATA denied and the producer would never
reach the produce path.

---

## Schema Registry RBAC Proxy (ApicurioRegistry)

`ApicurioRegistry` optionally deploys a Quarkus HTTP proxy (`apicurio-rbac-proxy`) in
front of the Apicurio Registry that enforces artifact-level access control.

```
Kafka client (curl / producer)
  │  Authorization: Bearer <JWT>
  ▼
apicurio-rbac-proxy:8082   (Quarkus OIDC + PolicyEngine)
  │  if allowed — strip Authorization header, forward
  ▼
apicurio-registry:8080     (Apicurio Registry, no auth)
```

### Request routing in `ProxyResource`

| Path pattern | Extracted artifact | Action |
|---|---|---|
| `GET /apis/registry/v2/groups/{g}/artifacts/{id}` | `{id}` | READ |
| `POST /apis/registry/v2/groups/{g}/artifacts/{id}/versions` | `{id}` | WRITE |
| `POST /apis/registry/v2/groups/{g}/artifacts` | `*` | WRITE |
| `DELETE /apis/registry/v2/groups/{g}/artifacts/{id}` | `{id}` | DELETE |
| `/schemas/{name}` (XML schema proxy) | `{name}` | method-based |

### PolicyEngine

- Loads `policy.yaml` (mounted from `{rbacRef}-apicurio-policy` ConfigMap) at startup.
- Watches the file with `java.nio.file.WatchService` and hot-reloads within ~1 s on change
  (no pod restart required when `KafkaRbac` is updated).
- `isAllowed(callerRoles, artifact, action)` returns `true` if any rule's `roles` intersect
  `callerRoles` and that rule grants `action` on `artifact` or `"*"`.

**Important:** HTTP/2 pseudo-headers (`:status`, `:path`) returned by the upstream Apicurio
Registry are filtered out before forwarding the response to the client. Vert.x rejects these
as invalid HTTP/1 header names.

### `kafkasql` storage backend

`ApicurioRegistry.spec.storage.type=kafkasql` durably stores schemas in a compacted
Kafka topic instead of `mem` (lost on restart) or `postgresql` (external DB). The
registry connects **directly to brokers** on the INTERNAL listener — not through
Kroxylicious, since the proxy mandates `SASL_SSL + OAUTHBEARER` and the registry
needs a long-lived workload identity.

```yaml
apiVersion: yavari.afshin.se/v1
kind: ApicurioRegistry
metadata:
  name: schema-registry
spec:
  storage:
    type: kafkasql
    clusterRef: my-kafka                # KafkaCluster in same namespace
    kafkaTopic: kafkasql-journal        # default if omitted
    tlsSecretRef: schema-registry-client-tls   # required when proxyMtls.enabled
    principal: apicurio-registry        # CN of the cert in tlsSecretRef
```

`tlsSecretRef` must be a cert-manager-style `kubernetes.io/tls` Secret with keys
`tls.crt`, `tls.key` (PKCS#8 PEM), and `ca.crt`. In production, mint it with a
cert-manager `Certificate` whose `commonName` matches `storage.principal`. In the
Kind test rig, `mcs-setup.sh` mints `schema-registry-client-tls` with CN
`apicurio-registry` automatically.

`storage.kafkaTopicPartitions` (optional, defaults to 1) overrides the journal
partition count. Apicurio v2.6 documents single-partition for ordering
guarantees; setting >1 is unsupported by Apicurio upstream but allowed by the
CRD for experimentation. The test rig sets it to 3 in
`kind/manifests/apicurio-kafkasql.yaml` to exercise the multi-partition
replication path; if schema reads turn inconsistent under load, revert to 1.

#### What the reconciler does

When `storage.type=kafkasql`, `ApicurioKafkasqlSupport` runs a preflight before
the Deployment is built:

1. Validates `clusterRef` exists; when the cluster has `proxyMtls.enabled=true`,
   also requires `tlsSecretRef` + `principal` and verifies the Secret has the
   three cert-manager keys.
2. Resolves the broker bootstrap via `BrokerBootstrapResolver` (alphabetically
   first broker pool's headless service on port 9092).
3. Server-side-applies a child `KafkaTopic` CR named
   `{registry}-kafkasql-journal` with `partitions=1`, `replicationFactor=3`,
   `cleanup.policy=compact`, `min.insync.replicas=2`, and
   `deletionPolicy=RETAIN` (deleting the registry CR does not wipe schema
   history). Blocks the Deployment until the topic is `READY`.
4. Provisions broker ACLs via `KafkaAclManager` for
   `User:CN={storage.principal}`:
   - `READ`/`WRITE`/`DESCRIBE` on the journal topic (literal),
   - `READ`/`DESCRIBE` on consumer groups prefixed `apicurio-registry`,
   - `DESCRIBE` on cluster.
5. Passes a `KafkasqlConfig` record to `ApicurioDeploymentBuilder`, which emits the
   `KAFKA_*` env vars (the names Apicurio v2.6 actually reads — see its
   `application.properties`: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`,
   `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SSL_*`). Apicurio's Kafka client SSL config
   only accepts file-based **PKCS12/JKS** keystores, not inline PEM, so the
   deployment adds a `pem-to-pkcs12` initContainer (using the cluster's Kafka
   image, which carries `openssl` + `keytool`) that converts the cert-manager
   PEM Secret into `/tmp/pkcs12/{keystore,truststore}.p12` (chmod 0644 so the
   registry's non-root UID can read them).

#### Broker authorizer + controller mTLS prerequisites

`kafkasql` requires broker-side ACL enforcement, so when
`KafkaCluster.spec.proxyMtls.enabled=true` the operator sets
`authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
and `allow.everyone.if.no.acl.found=false` in `server.properties` on **both
brokers and controllers** — in KRaft, ACL writes flow broker → active controller,
so both processes must run the authorizer. This also requires
`KafkaCluster.spec.controllerTls.mutualTls=true`: without mTLS on the
inter-controller CONTROLLER listener, controller-to-controller Raft traffic
arrives as `User:ANONYMOUS` and is denied, blocking quorum.

All cluster-internal mTLS certs (broker INTERNAL listener, per-pod CONTROLLER
listener, operator AdminClient) share the `CN=kafka-proxy` identity, which is
listed in `super.users`. This avoids needing per-pool CN entries in
`super.users`. Per-pod `{podName}-tls` Secrets must exist for every Kafka pod;
`mcs-setup.sh` mints them in the test rig (cert-manager `Certificate` resources
would handle this in production).

On `ApicurioRegistry` deletion the helper deletes the ACLs by principal; the
auto-created `KafkaTopic` CR is removed via ownerReference cascade, while the
underlying Kafka topic is retained per `deletionPolicy=RETAIN`.

---

## Kafka UI (React + Vite SPA + Quarkus 3.17 backend)

The `kafka-editor/` module is a React 19 + Vite SPA bundled into a Quarkus
backend that lets authenticated users browse and operate the Kafka cluster
through a visual editor and a topic/group/schema browser. It does **not**
hold privileged credentials of its own; every Kafka/Apicurio call carries
the logged-in user's JWT, so the existing `GroupAwareAuthorizer` +
`apicurio-rbac-proxy` enforce access. ACL editing remains out of scope —
`KafkaRbac` CRs are managed via GitOps.

The UI is deployed via the unchanged **`KafkaUI` CRD** (`kui`, group
`kafka.yavari.afshin.se`) — see
[api-reference.md → KafkaUI](api-reference.md#kafkaui). The operator
reconciler (`se.afshin.yavari.kafka.operator.ui`) materializes the
ServiceAccount, namespaced Role/RoleBinding, Deployment, Service, and optional
Ingress; child resources cascade via ownerReferences on `kubectl delete kui …`.
The OIDC client-secret Secret stays out of the operator surface so external
secret managers can own it.

```
Browser ──OIDC code+PKCE──► Keycloak (realm "demo", client "kafka-ui-web")
   │                            │
   ▼                            ▼
kafka-editor (React SPA at /, Quarkus REST under /api/*)
   │
   ├── AdminClient ─SASL_SSL/OAUTHBEARER + PEM mTLS → KafkaProxy
   ├── KafkaConsumer ─SASL_SSL/OAUTHBEARER + PEM mTLS → KafkaProxy
   ├── HTTP GET (Bearer JWT) ──────────────────────► apicurio-rbac-proxy
   └── KubernetesClient (read-only) ──────────────► KafkaRbac CRs
```

The frontend is a single-page React app (xyflow canvas + zustand store) that
talks to the backend exclusively under `/api/*`. The Quarkus backend serves
the built SPA from `META-INF/resources/index.html`; an HTTP-permission rule
forces the browser through Keycloak before the static handler hands over
`/`, so an unauthenticated client never sees the SPA shell. Static assets
under `/assets/*` stay public (they hold no secrets) and load on the
post-login round-trip.

### REST surface (Quarkus)

`/api/health`, `/q/health/{live,ready}` are public; everything else is
`@Authenticated`. Key paths (the SPA is the only intended consumer):

| Prefix | Purpose |
|---|---|
| `/api/admin/cluster` | Broker dashboard + reachability probe |
| `/api/admin/topics` | List / create / configure / delete topics, list partitions |
| `/api/admin/messages` | Paginated browse + produce + replay |
| `/api/admin/groups` | List, describe, delete consumer groups, reset offsets |
| `/api/admin/acls` | Read/write ACLs (proxy enforces the bearer's authorization) |
| `/api/admin/connect` | Kafka Connect proxy (no-op when no Connect URL configured) |
| `/api/registry/*` | Schema-registry browse + write proxy |
| `/api/run` | Run a visual-editor topology against TopologyTestDriver |
| `/api/me/rbac` | Self-introspection: `UserRbac` derived from KafkaRbac CRs |

Every write call emits one JSON line on the `kafka-editor.audit` logger via
`AuditFilter` (fields: `ts`, `user`, `action` (`http.<method>`), `target`
(URI path), `outcome`, `status`, `correlationId`). Read calls are audited at
the proxy layer (see [unified audit](audit.md)).

### Multi-user JWT propagation

The bearer token is carried inline in the kafka-clients `sasl.jaas.config`
line as a `rawToken=` option, salted with a per-request UUID to defeat
kafka-clients' login-subject cache. A custom `JwtCallbackHandler` reads the
token at configure time and synthesises an `OAuthBearerToken`. No ThreadLocal
is involved, so AdminClient's internal IO thread sees a stable per-instance
identity even under concurrent requests from different users (covered by
`JwtCallbackHandlerTest.concurrentHandlers_doNotCrossTokens`).

### OIDC roles: read from the access token, not the ID token

Quarkus OIDC in `application-type=web-app` extracts roles from the **ID
token** by default. Keycloak puts `realm_access.roles` on the access token
(and on the ID token only when a dedicated mapper is configured). The UI
therefore sets:

```properties
quarkus.oidc.roles.source=accesstoken
quarkus.oidc.roles.role-claim-path=realm_access/roles
```

Without `roles.source=accesstoken`, `SecurityIdentity.getRoles()` is empty
even though the user is authenticated — every topic and schema list looks
empty because the RBAC filter has nothing to allow. This was the single
trickiest piece of OIDC wiring to get right.

### Transport: SASL_SSL + mTLS to the proxy

The KafkaProxy's gateway uses `CnSubjectBuilderService`, so the transport
layer must be mTLS. The kafka-editor pod mounts the operator-generated
`kafka-proxy-test-client-tls` Secret (PEM files: `tls.crt`, `tls.key`,
`ca.crt`) at `/etc/kafka-tls` and `AdminClientFactory` passes them inline
via `ssl.keystore.type=PEM` + `ssl.keystore.certificate.chain` (and
`ssl.truststore.certificates`), which avoids any PKCS12 conversion step.
Authorization is still driven by the per-user SASL/OAUTHBEARER JWT — mTLS
is just the transport gate.

### Logout

`quarkus.oidc.logout.path=/logout` exposes an RP-initiated logout endpoint
that drops the Quarkus session cookie and redirects to Keycloak's
end-session endpoint to terminate the SSO session too. Without this,
clearing the local cookie alone leaves a Keycloak SSO cookie that silently
re-logs the user back in.

## MirrorMaker2 (cross-cluster replication)

The `MirrorMaker2` CRD is a **standalone** CRD parallel to `KafkaUI` — it is
not a sub-spec of `KafkaCluster`. One CR drives one replication flow
(`source -> target`). The reconciler renders an `mm2.properties` file into a
ConfigMap and drives a single Deployment that runs
`bin/connect-mirror-maker.sh` (Connect's dedicated MM2 driver). Workers across
all replicas join one Connect group via the `mm2-{configs,offsets,status}.{flow}`
topics on the **target** Kafka; Kafka's group coordinator distributes the
MirrorSource / MirrorCheckpoint / MirrorHeartbeat tasks.

```
source proxy bootstrap            target proxy bootstrap
        │                                    │
        │  records (Apicurio V3 envelope)    │
        ▼                                    ▼
   ┌─────────────────────────────────────────────┐
   │ MM2 worker Deployment (replicas=1|3)        │
   │   ┌──────────────────────────────────────┐  │
   │   │ MirrorSourceConnector                │  │
   │   │   transforms.schemaSync (optional)   │──┼── source Apicurio  GET /ids/globalIds/{id}
   │   │     • parse 0x00 + 8-byte globalId   │  │   target Apicurio  POST /groups/{g}/artifacts
   │   │     • DFS resolve refs               │  │
   │   │     • rewrite envelope globalId      │  │
   │   ├──────────────────────────────────────┤  │
   │   │ MirrorCheckpointConnector            │  │
   │   │ MirrorHeartbeatConnector             │  │
   │   └──────────────────────────────────────┘  │
   └─────────────────────────────────────────────┘
```

### Endpoint resolution

`Mm2EndpointResolver` translates each side of the CR (`spec.source`,
`spec.target`) into a `ResolvedEndpoint`:

- **Managed (`kafkaClusterRef`)**: looks up the referenced `KafkaCluster`,
  resolves bootstrap to the proxy Service (not broker headless) —
  `kafka-proxy.{ns}.svc.cluster.local:9094` — and pulls the admin client
  cert from `proxyMtls.adminClientCertSecretRef` (default
  `kafka-operator-client-tls`). When the cluster has Apicurio configured,
  the schema-registry URL is derived from
  `apicurio-rbac-proxy.{ns}.svc.cluster.local:8080`.
- **External**: passes through `bootstrap`, `tlsSecretRef`, `sasl`, and
  `schemaRegistry` verbatim.

CONFLUENT schema-registry type is rejected at reconcile (wire format
differs; reserved for a future release).

### Schema-sync SMT

The bundled `ApicurioSchemaTransferSmt` (in `schema-sync-smt/`) is wired
into the source→target transforms chain when `spec.schemaSync.enabled=true`
AND both ends expose a schema registry URL. Per record:

1. Topic regex check (`applyToTopics`).
2. Envelope detection — `bytes != null && length ≥ 9 && bytes[0] == 0x00`.
3. Parse 8-byte big-endian globalId.
4. Cache lookup → on miss, DFS resolve the source artifact (with
   `HashSet<Long> visiting` cycle guard, `maxDepth=16`) and recursively
   upsert references on the target before the parent
   (`?ifExists=FIND_OR_CREATE_VERSION`).
5. Rewrite the envelope's 8 bytes with the target globalId; payload
   unchanged.

Six layers of safety make the SMT safe to drop on mixed-format clusters:
per-record detection, tombstone passthrough, `applyTo` knob, topic
allowlist, default `behavior.on.error=WARN`, and per-record evaluation. See
[api-reference.md#non-apicurio-topics](api-reference.md#non-apicurio-topics).

### Schema-registry authentication

The SMT **writes** mirrored schemas to the target registry. A managed
cluster's Apicurio sits behind its `apicurio-rbac-proxy`, which OIDC-gates
every request — writes need an authenticated `schema-admin` identity.
Setting `schemaRegistryAuthSecretRef` on an endpoint points the SMT at a
Secret of OAuth2 client-credentials (`token-url` / `client-id` /
`client-secret`); the SMT runs the `client_credentials` grant and refreshes
the token before expiry and on a 401/403 (`OAuthTokenProvider`). The
operator mounts the Secret at `/etc/mm2/registry-auth/{source,target}/` and
passes the SMT only the directory path — never the secret values.

### Internal topics

The reconciler creates three KafkaTopic CRs on the target managed cluster
(`mm2-configs.{flow}` p=1, `mm2-offsets.{flow}` p=25, `mm2-status.{flow}` p=5,
all `cleanup.policy=compact`). Owner-refs cascade to the MM2 CR. When the
target is external, topic creation is skipped and the worker auto-creates
on first start.

### Replicas and placement

Default replicas = 3 when the target is a multi-cluster managed KafkaCluster
(spreads across MCS clusters via topology spread on
`topology.kubernetes.io/zone`); otherwise 1. Workers form one Connect group
via the internal topics, so distributing replicas across K8s clusters in
MCS gives single-K8s-cluster fault tolerance for the same flow.

### Where this lives

- `src/main/java/se/afshin/yavari/kafka/operator/mm2/` — reconciler + builders
- `src/main/java/se/afshin/yavari/kafka/operator/crd/MirrorMaker2*.java` — CRD classes
- `schema-sync-smt/` — Connect SMT JAR (shaded with Jackson); also carries
  `OAuthTokenProvider` and the `Mm2MirrorProbe` e2e helper
- `mm2-image/Dockerfile` — kafka-ubi:4.0.0 + the SMT JAR
- `kind/mm2-smoke-test.sh` — smoke e2e (CRD + reconciler wiring)
- `kind/mm2-mirror-test.sh` — extended e2e: real data + schema mirror
  (external source → managed target) exercising the SMT's OAuth write path
- See [api-reference.md#mirrormaker2](api-reference.md#mirrormaker2) for the
  full CRD reference.

## Backup / Restore (KafkaBackup, KafkaRestore, KafkaBackupValidation)

The operator does not implement a backup engine — it wraps the open-source
osodevops/kafka-backup tool, exactly as MirrorMaker2 wraps Kafka Connect.

- `KafkaBackupReconciler` renders an osodevops YAML config (`BackupConfigBuilder`),
  wraps it in a ConfigMap, and applies a `CronJob` (`BackupWorkloadBuilder`).
  Kubernetes owns the schedule cadence.
- `KafkaRestoreReconciler` / `KafkaBackupValidationReconciler` build one-shot
  `Job`s. Both are idempotent — a terminal `status.phase` blocks Job re-creation,
  so a reconcile triggered by anything else never re-runs a finished restore.
- `BackupEndpointResolver` resolves the managed cluster to a **direct broker
  headless** bootstrap (not the proxy) — bulk full-topic reads stay off the
  shared Kroxylicious proxy. mTLS PEM material is reused from `proxyMtls`.
- `BackupPlacementGate` enforces `spec.placement.clusterId` so the same CR
  applied to N MCS clusters runs the workload exactly once.
- When `includeSchemas` is set, `SchemaBackupStep` adds an Apicurio export
  sidecar (backup) / import init container (restore) to the pod, using the
  scripts bundled in the kafka-backup image.

### Where this lives

- `src/main/java/se/afshin/yavari/kafka/operator/backup/` — reconcilers + builders
- `src/main/java/se/afshin/yavari/kafka/operator/crd/KafkaBackup*.java`,
  `KafkaRestore*.java`, `KafkaBackupValidation*.java` — CRD classes
- `kafka-backup-image/` — osodevops kafka-backup compiled from source on a UBI
  base, plus the Apicurio export/import scripts
- `kind/kafka-backup-test.sh` — smoke e2e (CRD + reconciler wiring)
- See [api-reference.md#kafkabackup](api-reference.md#kafkabackup) for the
  full CRD reference.

---

## Observability

Two layers of Prometheus metrics:

- **Operator metrics** — Quarkus Micrometer endpoint at `:8080/metrics`. Counters/timers
  for rolling updates, ISR checks, scale-down, plus JVM defaults. Always on.
- **Data-plane metrics** — opt-in via `KafkaCluster.spec.metricsConfig` (and
  `MirrorMaker2.spec.metricsConfig` for MM2, a separate CRD). When set, the operator
  creates a dedicated `<name>-metrics` ClusterIP Service and a
  `monitoring.coreos.com/v1` ServiceMonitor for every relevant workload via the shared
  `MetricsResources` helper.

Each workload exposes metrics differently:

| Workload | Mechanism | Port |
|----------|-----------|------|
| Kafka brokers / controllers | `jmx_prometheus_javaagent` reading a user ConfigMap (`metricsConfig.configMapRef`) | 9101 |
| Kroxylicious proxy | Native — `management.endpoints.prometheus` in the proxy config | 9190 |
| Cruise Control | `jmx_prometheus_javaagent` + operator-bundled JMX config | 9101 |
| MirrorMaker2 (Connect) | `jmx_prometheus_javaagent` + operator-bundled JMX config | 9101 |

For the brokers, the user supplies the JMX exporter rules because they typically want
to tune the metric set. For CC and MM2 the MBean set is fixed, so the operator ships a
sensible default rules file (`src/main/resources/metrics/{cruise-control,connect}-jmx-config.yaml`)
and `MetricsConfig.configMapRef` is ignored for those workloads. The proxy needs no JMX
config at all — Kroxylicious exposes Prometheus natively over HTTP.

ServiceMonitor application goes through `OptionalResourceApplier`, which silently
no-ops when the Prometheus Operator CRD is absent — making the operator safe to deploy
on clusters without Prometheus.

---

## Audit logging

A cross-cutting JSON audit stream is emitted from inside both authorisation paths:

```
  Kafka client                           HTTP client
       │                                     │
       ▼                                     ▼
  ┌──────────────────┐               ┌──────────────────┐
  │  Kroxylicious    │               │ Apicurio         │
  │  filter chain    │               │ rbac-proxy       │
  │  ...             │               │   ProxyResource  │
  │  authorization   │               │   .proxy()       │
  │  audit  ────────┐│               │   try { ... }    │
  └─────────────────││               │   finally {      │
                    ▼▼                    audit.emit(…) │
        ┌─────────────────────┐      │   }              │
        │  AuditEmitter       │◀─────┘                  │
        │  (filters/kroxy/    │                         │
        │   audit/)           │                         │
        └────────┬────────────┘                         │
                 ├── StdoutAuditEmitter ──► stdout (always on)
                 └── KafkaAuditEmitter  ──► broker INTERNAL :9092
                                            (direct, mTLS, never via Kroxylicious)
```

The emitter library lives in the existing `filters/` module under
`se.afshin.yavari.kroxy.audit.*`. Both Kroxylicious (as a `Filter` sitting last in
the chain, observing the authorisation filter's response error codes) and the
Apicurio rbac-proxy (via a CDI-produced `AuditEmitter`) share the schema and the
sink fan-out.

The Kafka-topic sink is opt-in via `KafkaCluster.spec.audit.kafkaTopic.enabled`.
The `AuditOrchestrator` (`audit/AuditOrchestrator.java`) upserts the
`KafkaTopic` and injects `KAFKA_AUDIT_*` env + the `audit-tls` Volume on the proxy +
Apicurio Deployments. The producer is non-blocking (`max.block.ms=0`); on failure it
falls back to stdout and emits one rate-limited WARN per minute.

See `docs/audit.md` for the event schema and `docs/security.md#audit-logging` for
the trust posture.
