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

Users create `KafkaRbac`, `KafkaProxy`, and `ApicurioRegistry`. The operator creates and owns all downstream resources.

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
