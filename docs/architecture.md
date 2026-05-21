# Architecture

## Overview

The kafka-operator is a Kubernetes operator built with [Java Operator SDK (JOSDK)](https://javaoperatorsdk.io/) and [Quarkus](https://quarkus.io/). It manages Apache Kafka 4.x clusters running in KRaft mode (no ZooKeeper) across multiple Kubernetes clusters connected by [Submariner](https://submariner.io/) MCS.

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

All reconcilers use JOSDK's `UpdateControl.patchStatus().rescheduleAfter(15s)` when work is still in progress, creating a self-healing loop.

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

- Controllers form a **single KRaft quorum** across all clusters. Each controller advertises its address via `*.svc.clusterset.local` DNS provided by Submariner MCS.
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

The same CR is applied to every K8s cluster; each operator filters by its own `KAFKA_CLUSTER_ID` env var. Operators on clusters listed in `targetClusters` reconcile fully; operators on other clusters set `status.phase=SKIPPED` with no resources created. When `mcs.enabled=true` the operator also creates a Submariner `ServiceExport` so cross-cluster clients can resolve the service via `<svc>.<ns>.svc.clusterset.local` — **but only for ClusterIP or Headless underlying Services.** Submariner Lighthouse rejects `LoadBalancer`-typed Services (`UnsupportedServiceType`); with `externalAccess.type=LOADBALANCER` the export is created but never aggregated, and clients reach each cluster via its own LB IP instead. Use `GATEWAY` or `INGRESS` modes if you need Lighthouse aggregation.

For Apicurio specifically: all replicas across all clusters share **one** kafkasql journal topic on the MCS broker pool. Apicurio v2.6 requires the journal to have `partitions=1` for total ordering — the operator warns if the override is > 1. The kafkasql client cert (`schema-registry-client-tls` Secret with `CN=apicurio-registry`) is distributed to every cluster by `mcs-setup.sh`, so all replicas authenticate as the same Kafka principal and share ACLs. Concurrent writes to the same artifact-version across clusters are resolved by Apicurio's optimistic concurrency (one client gets a `409 Conflict`); this is rare and safe.

For Kafka UI: state is read-mostly (per-pod Caffeine cache, OIDC session local to each pod). External clients pin to one cluster's LB IP / Ingress host; for cross-cluster fallback, internal callers can resolve `kafka-ui.kafka.svc.clusterset.local` via Lighthouse.

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
  ├── (controller pool) CrossClusterRollCoordinator.isMyTurnToRoll()?
  │     Polls GET /operator/upgrade-phase on preceding clusters in clusterRollOrder.
  │     Blocks until all predecessors report upgradePhase=IDLE.
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

The blast radius of a rotation is just the pool / Deployment whose Secret changed. Cross-cluster ordering still follows `KafkaCluster.spec.clusterRollOrder`, so a synchronized rotation across all three MCS clusters still rolls one cluster at a time.

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

### Operation aliases

`KafkaProxy` RBAC rules use semantic names; `GroupAwareAuthorizer.matchesOp()` maps them:

| Semantic | Kroxylicious `TopicResource` operations |
|----------|-----------------------------------------|
| `PRODUCE` | `WRITE`, `DESCRIBE` |
| `FETCH` | `READ`, `DESCRIBE` |

`DESCRIBE` is needed because a Kafka producer first sends a `METADATA` request, which
Kroxylicious authorises as `DESCRIBE`. Without this alias, METADATA requests fail even for
groups that are allowed to produce.

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

## Kafka UI (Quarkus + htmx)

The `kafka-ui` sibling module is a single-instance Quarkus web app that lets
authenticated users browse the Kafka cluster(s) through a server-rendered HTML
UI. It does **not** hold privileged credentials of its own; every request to
the proxy or the schema registry carries the logged-in user's JWT, so the
existing `GroupAwareAuthorizer` + `apicurio-rbac-proxy` enforce all access.

The UI is deployed via the **`KafkaUI` CRD** (`kui`, group `kafka.yavari.afshin.se`)
— see [api-reference.md → KafkaUI](api-reference.md#kafkaui). The operator
reconciler (`se.afshin.yavari.kafka.operator.ui`) materializes the
ServiceAccount, namespaced Role/RoleBinding, Deployment, Service, and optional
Ingress; child resources cascade via ownerReferences on `kubectl delete kui …`.
The OIDC client-secret Secret stays out of the operator surface so external
secret managers can own it.

```
Browser ──OIDC code+PKCE──► Keycloak (realm "demo", client "kafka-ui-web")
   │                            │
   ▼                            ▼
kafka-ui (Quarkus + Qute + htmx)
   │
   ├── AdminClient ─SASL_PLAINTEXT/OAUTHBEARER → KafkaProxy (per-CR Service)
   ├── KafkaConsumer ─SASL_PLAINTEXT/OAUTHBEARER → KafkaProxy
   ├── HTTP GET (Bearer JWT) ─────────────────► apicurio-rbac-proxy (per-CR Service)
   └── KubernetesClient (read-only) ─────────► KafkaCluster / KafkaRbac CRs
```

### Endpoints

| Path | Purpose |
|---|---|
| `GET /` | Cluster picker (lists `KafkaCluster` CRs) |
| `GET /clusters/{id}` | Broker dashboard |
| `GET /clusters/{id}/topics` | Topic list, filtered by KafkaRbac |
| `GET /clusters/{id}/topics/{name}` | Topic configs + partitions |
| `GET /clusters/{id}/topics/{name}/messages` | Paginated message browser with auto-deserializer |
| `GET …/messages/stream` | SSE live tail |
| `GET /clusters/{id}/groups` | Consumer groups |
| `GET /clusters/{id}/acls` | KafkaRbac rules (header copy: "Access rules") |
| `GET /clusters/{id}/schemas` | Apicurio artifact list |
| `GET /clusters/{id}/schemas/{id}` | Artifact content + versions |

### Smart deserializer

Pure first-hit-wins detection on the value/key bytes:

1. `null` payload → tombstone badge
2. `0x00 || globalId:int64` → Apicurio V3 envelope, schema fetched by globalId
3. `0x00 || schemaId:int32` → Confluent envelope, fetched by id
4. Apicurio artifact lookup by `{topic}-{key|value}` naming convention
5. JSON heuristic (starts with `{` or `[` and parses)
6. UTF-8 heuristic (≥95% printable; strict decode — no replacement char accepted)
7. Hex dump (first 256 bytes)

Schema fetches are cached in-process (long TTL by globalId; 30 s for
name-based lookups, including negative results). Avro decodes via
`GenericDatumReader → JsonEncoder`; JSON schema artifacts pretty-print the
payload; Protobuf renders as hex with a Phase-2 TODO.

### Multi-cluster routing

The UI lists *KafkaCluster CRs* — those are the logical clusters. The 3 MCS
K8s clusters (kafka-a/b/c) backing one CR are invisible to the UI; Submariner's
local-prefer routing through the aggregated `{service}.{ns}.svc.clusterset.local`
DNS name picks a reachable replica. Per-CR Service names
(`kafka-proxy-{crName}`, `apicurio-rbac-proxy-{crName}`) make different CRs
addressable independently.

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
layer must be mTLS. The UI mounts the operator-generated
`kafka-proxy-test-client-tls` Secret (PEM files: `tls.crt`, `tls.key`,
`ca.crt`) at `/etc/kafka-tls` and `KafkaClientProvider` passes them inline
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
