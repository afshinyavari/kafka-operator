# API Reference

All CRDs are in group `kafka.yavari.afshin.se`, version `v1alpha1`.

## CRD overview

| Kind | Short name | Purpose | References other CRDs |
|------|------------|---------|-----------------------|
| [KafkaCluster](#kafkacluster) | `kc` | Cluster-wide configuration and KRaft quorum definition. One per K8s cluster; the `spec.clusters` list is identical on every cluster in the quorum. | — |
| [KafkaNodePool](#kafkanodepool) | `knp` | A pool of pods sharing role (`CONTROLLER` / `BROKER`), storage, and resources. Linked to its `KafkaCluster` via the `kafka.yavari.afshin.se/cluster=<name>` label. | KafkaCluster (via label) |
| [KafkaPodSet](#kafkapodset) | `kps` | Operator-managed pod set created from a KafkaNodePool. Do not edit. | — |
| [KafkaProxy](#kafkaproxy) | `kp` | Kroxylicious proxy in front of a broker pool. Optional SASL/OAUTHBEARER, RBAC, schema validation, and MCS multi-cluster deployment. | KafkaCluster, KafkaNodePool, KafkaRbac, ApicurioRegistry |
| [KafkaRbac](#kafkarbac) | `kra` | Declarative topic ACLs and schema-registry artifact ACLs in a single CR. The operator turns this into ConfigMaps for Kroxylicious and the Apicurio RBAC proxy. | — |
| [ApicurioRegistry](#apicurioregistry) | `apr` | Apicurio Registry deployment with an optional JWT-aware RBAC proxy. | KafkaRbac |
| [KafkaUI](#kafkaui) | `kui` | Web UI Deployment + Service + RBAC (Role/RoleBinding/SA) for browsing Kafka clusters through Keycloak SSO. | KafkaCluster (read-only at runtime, no CRD-level ref) |
| [KafkaTopic](#kafkatopic) | `kt` | Declarative Kafka topic — partitions, replication factor, dynamic config. Reconciled by the operator instance running on the primary K8s cluster (`spec.clusters[0].id` on the referenced `KafkaCluster`); peer instances mark the CR `SKIPPED`. | KafkaCluster |
| [KafkaBackup](#kafkabackup) | `kbk` | Scheduled backup of topic data (and Apicurio schemas) to object storage. The operator builds a `CronJob` running the osodevops kafka-backup tool. | KafkaCluster |
| [KafkaRestore](#kafkarestore) | `krs` | One-shot, idempotent restore of a KafkaBackup into a managed cluster. | KafkaBackup, KafkaCluster |
| [KafkaBackupValidation](#kafkabackupvalidation) | `kbv` | One-shot integrity check of a stored backup. | KafkaBackup |
| [KafkaRebalance](#kafkarebalance) | `krb` | Declarative Cruise Control rebalance — generates an optimization proposal, approved via annotation, then executed and tracked to completion. | KafkaCluster |

---

## KafkaCluster

Cluster-scoped configuration and KRaft quorum definition. One `KafkaCluster` CR is deployed per K8s cluster; all clusters in the quorum must carry the same `spec.clusters` list.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `kafkaImage` | string | no | `kafka-ubi:4.0.0` | Container image for all Kafka pods. |
| `kafkaVersion` | string | no | `"4.0"` | Kafka version string. Used for upgrade tracking and downgrade protection. |
| `targetMetadataVersion` | integer | no | — | When set, the operator bumps `metadata.version` to this value after all pods are on `kafkaVersion`. Must not be lower than `status.currentMetadataVersion`. |
| `clusters` | []ClusterEntry | **yes** | — | Ordered list of all clusters in the KRaft quorum. Index position determines the controller `node.id` (`10000 + index`). At least 1 entry required. |
| `config` | map[string]string | no | `{}` | Shared Kafka config properties applied to all node pools. Operator-owned keys (see below) are silently overridden. |
| `metricsConfig` | MetricsConfig | no | — | Enables Prometheus scraping for the cluster. For brokers it starts `jmx_prometheus_javaagent` reading the user-supplied ConfigMap and creates a `<pool>-metrics` ServiceMonitor. When set, the operator **also** creates `kafka-proxy-metrics` (Kroxylicious native `/metrics` on 9190) and `cruise-control-metrics` (JMX exporter with operator-bundled config, on 9101) when those sub-components are deployed. `configMapRef` is consulted only by the broker exporter; the proxy and CC ignore it. |
| `clusterRollOrder` | []string | no | — | Ordered list of cluster IDs (matching `spec.clusters[].id`) for cross-cluster rolling coordination. First cluster in the list rolls first. Absent = no coordination. |
| `listeners` | []KafkaListenerSpec | no | `[]` | Additional client-facing listeners beyond the always-present `INTERNAL:9092`. When non-empty, `INTERNAL` binds to `127.0.0.1` only and the first entry becomes `inter.broker.listener.name`. |
| `controllerTls` | KafkaListenerTlsConfig | no | — | Enables TLS on the KRaft `CONTROLLER:9093` listener. When set, all nodes load their TLS secret at startup. |
| `proxyMtls` | KafkaProxyMtlsConfig | no | — | Enables mTLS on the broker `INTERNAL` listener for a Kroxylicious-style proxy. Driven from the cluster spec (not KafkaProxy presence) so every cluster in the MCS topology reconciles consistently, even when the proxy Deployment only runs on one cluster. |
| `cruiseControl` | KafkaClusterCruiseControlSpec | no | — | Deploys LinkedIn Cruise Control for partition rebalancing. When set, the Cruise Control Metrics Reporter is added to every broker (a one-time rolling restart) and one Cruise Control Deployment runs on the primary cluster. |
| `audit` | KafkaClusterAuditSpec | no | — | Unified audit logging. The in-process audit emitter is **always on** in both Kroxylicious and the Apicurio rbac-proxy (one JSON line per request on the `kafka-audit` SLF4J channel). This sub-spec opts into the Kafka-topic sink and allows trimming cardinality with an op allowlist. See `docs/audit.md` for the event schema. |

### spec.clusters[] — ClusterEntry

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | **yes** | Logical cluster ID. Must match the `KAFKA_CLUSTER_ID` env var on that cluster's operator deployment. |
| `controllerAdvertisedAddress` | string | **yes** | `host:port` for the controller's quorum listener. Use `{pool}-headless.kafka.svc.clusterset.local:9093` in MCS mode. |
| `operatorAddress` | string | no | `host:port` for the operator's HTTP endpoint. Required when `clusterRollOrder` is set; used for cross-cluster upgrade-phase polling. |

### spec.metricsConfig — MetricsConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `configMapRef` | string | **yes** (for brokers) | Name of a `ConfigMap` in the same namespace with a `jmx-config.yaml` key containing the JMX exporter config. Consulted **only** by the broker `jmx_prometheus_javaagent`. The proxy uses Kroxylicious' native Prometheus endpoint, and Cruise Control + MirrorMaker2 use operator-bundled JMX configs — for those, presence of `metricsConfig` alone enables metrics. |

### spec.listeners[] — KafkaListenerSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | **yes** | — | Listener name, uppercase alphanumeric + underscores (e.g. `CLIENT_TLS`). Must not be `INTERNAL` or `CONTROLLER`. |
| `port` | integer | **yes** | — | Port number. Must not conflict with 9092 (`INTERNAL`) or 9093 (`CONTROLLER`). |
| `tls` | KafkaListenerTlsConfig | no | — | When present, listener uses SSL protocol and PKCS12 keystores. |
| `externalAccess` | enum | no | — | `null` = internal-only (binds `0.0.0.0`). `NODEPORT` = operator creates one NodePort Service per broker pod. `LOADBALANCER` = reserved, not yet implemented. |
| `nodePortBase` | integer | no | `31000` | Base nodePort for the `NODEPORT` type. Broker at ordinal `N` gets `nodePortBase + N`. Must be in the Kubernetes NodePort range (30000–32767). |

### spec.controllerTls / spec.listeners[].tls — KafkaListenerTlsConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mutualTls` | boolean | no | `false` | When `true`, sets `client.auth=required` — clients must present a certificate signed by the CA in the TLS secret. |

TLS requires a Secret named `{podName}-tls` in the same namespace with keys `tls.crt`, `tls.key`, and `ca.crt` (cert-manager convention). The operator converts them to PKCS12 keystores at pod startup.

### spec.proxyMtls — KafkaProxyMtlsConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `false` | Enables mTLS on the broker `INTERNAL` listener. When `true`, the operator expects a pre-provisioned TLS secret per broker pool (cert-manager in production, `mcs-setup.sh` in tests). |
| `proxyPrincipal` | string | no | `"kafka-proxy"` | The CN the proxy presents on its client cert. Added to `super.users` so the proxy has unrestricted access. |

### spec.audit — KafkaClusterAuditSpec

The in-process emitter writes one JSON line per Kafka request (Kroxylicious) or HTTP request (Apicurio rbac-proxy) to a dedicated SLF4J channel `kafka-audit` at INFO. That stream is always on — operators can ship it via Fluent Bit / Vector / Promtail to any log backend.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `kafkaTopic` | AuditKafkaTopicSpec | no | — | Opts into the Kafka-topic sink. When unset, stdout is the only sink. |
| `includeOps` | []string | no | `[]` | Allowlist of operation names to emit (`PRODUCE`, `FETCH`, `CREATE_TOPICS`, `READ`, `WRITE`, `DELETE`, ...). Empty/omitted = emit every operation. Use this to drop the high-volume verbs (typically `FETCH`) so the `__audit` topic stays useful. |

#### spec.audit.kafkaTopic — AuditKafkaTopicSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `false` | Master switch for the Kafka-topic sink. When `true`, the operator upserts a `KafkaTopic` named `<cluster>-audit` and injects `KAFKA_AUDIT_*` env (with mTLS PEM paths) on the proxy + Apicurio Deployments. Audit traffic goes direct to the broker `INTERNAL` listener — never via Kroxylicious. |
| `name` | string | no | `__audit` | Kafka topic name. Must match `[a-zA-Z0-9._-]{1,249}`. |
| `retentionDays` | integer | no | `30` | Topic retention (1–365). Maps to `retention.ms`. |
| `partitions` | integer | no | `3` | Topic partition count (≥1). |
| `replicationFactor` | integer | no | `3` | Topic replication factor (≥1). |

The audit event schema is documented in `docs/audit.md`.

### spec.cruiseControl — KafkaClusterCruiseControlSpec

Deploys LinkedIn Cruise Control as a **singleton** — one Deployment + ClusterIP Service on the primary cluster (`spec.clusters[0]`); every other cluster reports `status.cruiseControl.phase=SKIPPED`. Enabling this sub-spec adds the Cruise Control Metrics Reporter to every broker's `metric.reporters`, which triggers a **one-time rolling restart** of all brokers.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `cruise-control-ubi:2.5.146` | Cruise Control container image (compiled from source on a UBI base). |
| `replicas` | integer | no | `1` | Cruise Control is a singleton; the operator pins the effective replica count to 1. |
| `resources` | ResourceRequirements | no | req `500m`/`1Gi`, lim `1`/`2Gi` | CPU/memory for the Cruise Control container. |
| `config` | map[string]string | no | `{}` | Extra `cruisecontrol.properties` entries. Operator-computed keys (bootstrap, TLS, capacity-file path, sampler) always win. |
| `goals` | []string | no | — | Ordered Cruise Control goal class names. Empty = Cruise Control's own default goal set. |
| `capacity` | CruiseControlCapacityConfig | no | — | Per-broker capacity inputs for `capacity.json`. |
| `apiSecurity` | CruiseControlApiSecurity | no | — | Optional HTTP basic auth on the Cruise Control REST API. |
| `metricsReporter` | CruiseControlMetricsReporterConfig | no | — | Tuning for the `__CruiseControlMetrics` topic. |
| `brokerClientCertSecretRef` | string | no | — | cert-manager PEM Secret for Cruise Control's AdminClient when broker mTLS is on. Null = reuse the operator's admin client cert (`proxyMtls.adminClientCertSecretRef`). |
| `principal` | string | no | — | CN of a dedicated Cruise Control cert; when set it is appended to broker `super.users`. Null = Cruise Control uses the shared operator identity (already a super-user). |

#### spec.cruiseControl.capacity — CruiseControlCapacityConfig

All fields optional; unset fields fall back to documented defaults.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `disk` | string | `100000` | Per-broker disk capacity in MB. |
| `cpu` | number | `100` | Per-broker CPU capacity (Cruise Control capacity units). |
| `inboundNetwork` | string | `100000` | Per-broker inbound network capacity in KB/s. |
| `outboundNetwork` | string | `100000` | Per-broker outbound network capacity in KB/s. |

#### spec.cruiseControl.apiSecurity — CruiseControlApiSecurity

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Require HTTP basic auth on the Cruise Control REST API. |
| `basicAuthSecretRef` | string | — | Secret with the Cruise Control `auth-credentials.properties` file plus `username`/`password` keys (the latter used by the `KafkaRebalance` reconciler). Required when `enabled`. |

#### spec.cruiseControl.metricsReporter — CruiseControlMetricsReporterConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `metricsTopicReplicas` | integer | `min(3, brokers)` | Replication factor for `__CruiseControlMetrics`. |
| `metricsTopicPartitions` | integer | reporter default | Partition count for `__CruiseControlMetrics`. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `RECONCILING` \| `READY` \| `DEGRADED` \| `FAILED` | Overall cluster state. |
| `message` | string | Human-readable status message. |
| `lastReconcileTime` | string | ISO-8601 timestamp of last reconcile. |
| `observedGeneration` | long | Generation of the spec last reconciled. |
| `poolPhases` | map[string]string | Per-pool readiness: `"ready/desired"` (e.g. `"3/3"`). |
| `currentKafkaVersion` | string | Kafka version currently running. |
| `upgradePhase` | string | `IDLE` \| `ROLLING` — used by the cross-cluster roll coordinator. |
| `currentMetadataVersion` | integer | `metadata.version` currently active in the cluster. |
| `cruiseControl` | CruiseControlStatus | Cruise Control sub-status (set when `spec.cruiseControl` is present): `phase` (`RECONCILING`/`READY`/`FAILED`/`SKIPPED`), `message`, `url` (in-cluster REST URL). `SKIPPED` on every non-primary cluster. |

### Operator-owned config keys

The following keys in `spec.config` are **silently overridden** by the operator and should not be set by the user:

`process.roles`, `node.id`, `cluster.id`, `controller.quorum.voters`, `listeners`, `advertised.listeners`, `listener.security.protocol.map`, `controller.listener.names`, `log.dirs`, `inter.broker.listener.name`, `controller.advertised.listeners`, and all `listener.name.*` SSL properties.

### Configuration recipes

#### Single-cluster (dev)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaCluster
metadata:
  name: my-kafka
  namespace: kafka
spec:
  kafkaVersion: "4.0"
  clusters:
    - id: A
      controllerAdvertisedAddress: controllers-headless.kafka.svc.cluster.local:9093
```

#### 3-cluster MCS quorum with roll ordering

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaCluster
metadata:
  name: my-kafka
  namespace: kafka
spec:
  kafkaVersion: "4.0"
  clusterRollOrder: ["A", "B", "C"]
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
```

#### Internal TLS listener + TLS on controller channel

```yaml
spec:
  listeners:
    - name: CLIENT_TLS
      port: 9094
      tls:
        mutualTls: false   # set true to require client certificates
  controllerTls:
    mutualTls: false
```

When `listeners` contains a TLS entry, `INTERNAL` binds to `127.0.0.1` (admin tools within the pod only) and `CLIENT_TLS` becomes the inter-broker listener. Each broker and controller pod requires a Secret named `{podName}-tls`.

#### External NodePort (plaintext, clients outside the cluster)

```yaml
spec:
  listeners:
    - name: EXTERNAL
      port: 9094
      externalAccess: NODEPORT
      nodePortBase: 31000
```

Produces services `{pool}-0-external-ext` (nodePort 31000), `{pool}-1-external-ext` (31001), …  Each broker advertises `{nodeIP}:{nodePort}`. `INTERNAL` remains the inter-broker listener (binds `0.0.0.0`).

#### Both internal TLS and external NodePort

```yaml
spec:
  listeners:
    - name: CLIENT_TLS
      port: 9094
      tls:
        mutualTls: false
    - name: EXTERNAL
      port: 9095
      externalAccess: NODEPORT
      nodePortBase: 31100
```

`CLIENT_TLS` is the inter-broker listener; `INTERNAL` is localhost-only for admin tools; `EXTERNAL` is advertised via node IP + NodePort.

#### mTLS to a Kroxylicious proxy

```yaml
spec:
  proxyMtls:
    enabled: true
    proxyPrincipal: kafka-proxy
```

Per-pool broker certs are read from `{poolName}-broker-tls` (override per pool with [`KafkaNodePool.spec.brokerCertSecretRef`](#spec-1)). The operator does **not** create these secrets — provision them with cert-manager (production) or `mcs-setup.sh` (tests). All clusters in the quorum should enable this together.

---

## KafkaNodePool

Defines a set of Kafka nodes with a common role, replica count, and resource configuration. Must carry the label `kafka.yavari.afshin.se/cluster=<KafkaCluster name>`.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `roles` | []enum | **yes** | — | At least one of `CONTROLLER`, `BROKER`. Both may be set for combined nodes. |
| `replicas` | integer | no | `1` | Number of pods. Minimum 1. |
| `storage` | StorageSpec | no | 10Gi | PVC spec for Kafka data (`/var/lib/kafka/data`). PVCs are retained on pod deletion and on scale-down. |
| `resources` | ResourceRequirements | no | — | Standard Kubernetes `resources` block (requests + limits for CPU and memory). |
| `config` | map[string]string | no | `{}` | Pool-level Kafka config overrides. Merged on top of `KafkaCluster.spec.config`; the same operator-owned key restrictions apply. |
| `rackTopologyKey` | string | no | — | Node label key whose value becomes `broker.rack` (e.g. `topology.kubernetes.io/zone`). When set, the operator reads node labels and assigns zones deterministically. |
| `brokerCertSecretRef` | string | no | `{poolName}-broker-tls` | Pre-provisioned TLS secret (cert-manager convention: `tls.crt` + `tls.key` + `ca.crt`) holding this pool's broker cert. Only used when [`KafkaCluster.spec.proxyMtls.enabled=true`](#specproxymtls--kafkaproxymtlsconfig). |

### spec.storage — StorageSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `size` | string | no | `"10Gi"` | PVC storage request (Kubernetes quantity string). |
| `storageClassName` | string | no | — | Storage class name. Omit to use the cluster default. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `PENDING` \| `RECONCILING` \| `READY` \| `FAILED` | Pool state. |
| `message` | string | Status message. |
| `readyReplicas` | integer | Number of ready pods. |
| `desiredReplicas` | integer | Target replica count from spec. |
| `lastError` | string | Last reconciliation error, if any. |

### Configuration recipes

#### Controller pool

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaNodePool
metadata:
  name: controllers
  namespace: kafka
  labels:
    kafka.yavari.afshin.se/cluster: my-kafka
spec:
  roles: [CONTROLLER]
  replicas: 1
  storage:
    size: 1Gi
  resources:
    requests: { cpu: 100m, memory: 256Mi }
    limits:   { cpu: 500m, memory: 512Mi }
```

#### Broker pool with NodePort external access

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers
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
    requests: { cpu: 500m, memory: 2Gi }
    limits:   { memory: 2Gi }
```

The external listener itself is declared on `KafkaCluster.spec.listeners` — see the [external NodePort recipe](#external-nodeport-plaintext-clients-outside-the-cluster).

#### Rack-aware broker pool

```yaml
spec:
  roles: [BROKER]
  replicas: 3
  rackTopologyKey: topology.kubernetes.io/zone
```

The operator reads node labels at reconcile time and assigns `broker.rack` deterministically so partition replicas spread across zones.

#### Broker pool with mTLS broker cert

```yaml
spec:
  roles: [BROKER]
  replicas: 3
  brokerCertSecretRef: my-brokers-broker-tls   # default would be "brokers-broker-tls"
```

Requires `KafkaCluster.spec.proxyMtls.enabled=true`. The secret must exist before the pool's pods start.

---

## KafkaPodSet

Operator-managed. **Do not create or modify directly.** One `KafkaPodSet` is created per `KafkaNodePool`; its name is `{poolName}-podset`.

### Useful status fields for debugging

| Field | Description |
|-------|-------------|
| `status.currentRollingPod` | Name of the pod currently being rolled. Empty string when no roll is in progress. If non-empty and not changing, a roll may be blocked by an ISR/quorum check. |
| `status.pods[].specHash` | Desired spec hash for this pod. |
| `status.pods[].currentSpecHash` | Hash of the running pod's spec. If these differ, a rolling update is pending or in progress. |
| `status.pods[].ready` | Whether the pod currently passes its readiness probe. |
| `status.pods[].nodeId` | Kafka broker node ID for this pod. |

---

## KafkaProxy

Deploys a Kroxylicious proxy in front of a broker pool. Clients connect to the proxy port (`spec.clientPort`) instead of directly to Kafka. The proxy can enforce SASL/OAUTHBEARER auth, RBAC, and message-payload validation. One `KafkaProxy` per cluster is the typical setup; the same CR can be applied to multiple K8s clusters in MCS mode and the operator on each cluster decides whether to deploy.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | string | **yes** | — | Name of the [`KafkaCluster`](#kafkacluster) CR this proxy serves. |
| `poolRef` | string | **yes** | — | Name of the broker [`KafkaNodePool`](#kafkanodepool) this proxy fronts. |
| `replicas` | integer | no | `1` | Number of proxy pods. |
| `image` | string | no | — | Kroxylicious container image (typically a custom build with the project's filter JAR). Required if no operator-level default is set. |
| `clientPort` | integer | no | `9094` | Port exposed to Kafka clients. |
| `rbacRef` | string | no | — | Name of a [`KafkaRbac`](#kafkarbac) CR. When set, `GroupAwareAuthorizerService` is activated and topic ACLs are enforced. |
| `apicurioRef` | string | no | — | Name of an [`ApicurioRegistry`](#apicurioregistry) CR. When set and the registry is ready, the proxy's record-validation filter can resolve schemas through it. |
| `brokerNodeIdRanges` | []BrokerNodeIdRange | no | `[]` | Explicit mapping of node-ID ranges to pool names. When omitted, the operator infers ranges by inspecting broker pod labels. |
| `oidc` | KafkaProxyOidcConfig | no | — | Enables SASL/OAUTHBEARER + JWT group extraction. When set, the four-filter OIDC chain is injected. |
| `tls` | KafkaProxyTlsConfig | no | — | TLS secrets the proxy mounts. When fields are null the reconciler falls back to convention defaults, so a typical CR omits this block entirely. |
| `mcs` | McsConfig | no | — | Enables MCS (`ServiceExport`) mode for cross-cluster client resolution. Works with any MCS-compatible mesh: Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster, etc. |
| `targetClusters` | []string | no | `[]` | List of cluster IDs (matching `KafkaCluster.spec.clusters[].id`) on which to deploy this proxy. Operators on other clusters set `status.phase=SKIPPED`. Used in MCS mode where the same CR is applied to every cluster. |
| `filters` | KafkaProxyFiltersConfig | no | (defaults) | Built-in filter toggles (XML validation, schema-registry payload validation). |
| `customFilters` | []KafkaProxyCustomFilter | no | `[]` | Arbitrary Kroxylicious filter entries appended verbatim to `config.yaml`. |
| `externalAccess` | KafkaProxyExternalAccessConfig | no | — | When set, the Service is exposed via LoadBalancer, Gateway API TLSRoute, or Ingress (ssl-passthrough). See section below. |

### spec.externalAccess — KafkaProxyExternalAccessConfig

Exposes the proxy outside the K8s cluster. All three modes resolve a per-cluster advertised
host (LoadBalancer auto-resolves from `Service.status.loadBalancer.ingress`; Gateway/Ingress
substitute `${clusterId}` in `advertisedHostTemplate`). For Gateway/Ingress the proxy switches
from `portIdentifiesNode` to `sniHostIdentifiesNode` and dispatches by TLS SNI hostname.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | enum | **yes** | — | `LOADBALANCER`, `GATEWAY`, or `INGRESS`. |
| `advertisedHostTemplate` | string | LB: no, GATEWAY/INGRESS: **yes** | — | Hostname the proxy advertises to clients. Supports `${clusterId}` substitution (lowercased). For LOADBALANCER, leave empty to auto-resolve from Service status. |
| `gateway` | KafkaProxyGatewayConfig | when `type=GATEWAY` | — | Parent Gateway reference. |
| `ingress` | KafkaProxyIngressConfig | no | — | Ingress controller hints (only used for `type=INGRESS`). |

#### spec.externalAccess.gateway — KafkaProxyGatewayConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `parentGatewayName` | string | **yes** | — | Name of the existing `gateway.networking.k8s.io` Gateway resource. |
| `parentGatewayNamespace` | string | no | proxy namespace | Namespace of the parent Gateway. |
| `sectionName` | string | no | — | Optional Listener `sectionName` on the parent Gateway. |

#### spec.externalAccess.ingress — KafkaProxyIngressConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `ingressClassName` | string | no | cluster default | Ingress class. The controller must support ssl-passthrough (e.g. nginx-ingress with `--enable-ssl-passthrough`). |

### spec.oidc — KafkaProxyOidcConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `jwksEndpointUrl` | string | no | — | JWKS URL for JWT signature verification (e.g. Keycloak `/certs` endpoint). Required in practice when the `oidc` block is set. |
| `groupsClaim` | string | no | `realm_access.roles` | Dot-separated JWT claim path containing the group/role list. |
| `expectedIssuer` | string | no | — | If set, the `iss` claim must match this value. |
| `expectedAudience` | string | no | — | If set, the `aud` claim must contain this value. |

### spec.tls — KafkaProxyTlsConfig

Both secrets follow the cert-manager convention (`kubernetes.io/tls` with `tls.crt` + `tls.key` + `ca.crt`). The operator does **not** create or sign these — provision them externally.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clientCertSecretRef` | string | no | `{proxyName}-client-tls` | Secret holding the proxy's client cert (presented upstream to brokers when `KafkaCluster.spec.proxyMtls.enabled=true`). |
| `serverCertSecretRef` | string | no | `{proxyName}-server-tls` | Secret holding the proxy's server cert (presented to downstream Kafka clients). |

### spec.mcs — McsConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `false` | When `true`, the operator creates an MCS `ServiceExport` for the proxy Service so cross-cluster clients resolve `kafka-proxy.<ns>.svc.clusterset.local` via the cluster fabric's MCS DNS. Used with `targetClusters`. |

### spec.filters — KafkaProxyFiltersConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `xmlValidation.enabled` | boolean | `false` | Enables the XML schema validation filter. Schemas are read from a Kafka topic. |
| `xmlValidation.schemaTopic` | string | — | Kafka topic name where XML schemas are stored. |
| `schemaRegistry.enabled` | boolean | `false` | Enables Kroxylicious's built-in `RecordValidation` filter for JSON/Avro/Protobuf schema checking via Apicurio. |
| `schemaRegistry.schemaType` | string | `JSON_SCHEMA` | `JSON_SCHEMA` \| `AVRO` \| `PROTOBUF`. |
| `schemaRegistry.topics` | []string | `[]` | Topic names to apply schema validation to. |

### spec.customFilters[] — KafkaProxyCustomFilter

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | no | Filter instance name (free-form). |
| `type` | string | no | Filter factory class name. |
| `config` | map[string]object | no | Arbitrary filter configuration appended to Kroxylicious `config.yaml`. |

### spec.brokerNodeIdRanges[] — BrokerNodeIdRange

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | no | Broker pool name. |
| `start` | integer | no | Inclusive start of node-ID range. |
| `end` | integer | no | Inclusive end of node-ID range. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `RECONCILING` \| `READY` \| `FAILED` \| `SKIPPED` | Proxy deployment state. `SKIPPED` means this cluster is not in `targetClusters`. |
| `message` | string | Status message or error detail. |
| `readyReplicas` | integer | Number of ready proxy pods. |

### Configuration recipes

#### Dev (plain, no auth)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaProxy
metadata:
  name: kafka-proxy
  namespace: kafka
spec:
  clusterRef: my-kafka
  poolRef: brokers
  image: kroxy-filters:dev
```

#### OIDC + group RBAC (single cluster)

```yaml
spec:
  clusterRef: my-kafka
  poolRef: brokers
  image: kroxy-filters:dev
  rbacRef: kafka-rbac
  oidc:
    jwksEndpointUrl: http://keycloak.kafka.svc.cluster.local:8080/realms/demo/protocol/openid-connect/certs
    groupsClaim: realm_access.roles
    expectedIssuer: http://keycloak.kafka.svc.cluster.local:8080/realms/demo
    expectedAudience: rbac-proxy
```

#### OIDC + RBAC + XML and schema-registry validation

```yaml
spec:
  clusterRef: my-kafka
  poolRef: brokers
  image: kroxy-filters:dev
  rbacRef: kafka-rbac
  apicurioRef: apicurio
  oidc:
    jwksEndpointUrl: http://keycloak.kafka.svc.cluster.local:8080/realms/demo/protocol/openid-connect/certs
  filters:
    xmlValidation:
      enabled: true
      schemaTopic: xml-schemas
    schemaRegistry:
      enabled: true
      schemaType: JSON_SCHEMA
      topics: [orders-json]
```

#### MCS multi-cluster deployment

The same CR is applied to every cluster in the MCS topology. Each operator deploys the proxy only if its local cluster ID is in `targetClusters`; others report `status.phase=SKIPPED`.

```yaml
spec:
  clusterRef: my-kafka
  poolRef: brokers-a
  image: kroxy-filters:dev
  mcs:
    enabled: true
  targetClusters: [A, B]
  brokerNodeIdRanges:
    - { name: brokers-a, start: 0,    end: 0 }
    - { name: brokers-b, start: 1000, end: 1000 }
    - { name: brokers-c, start: 2000, end: 2000 }
  rbacRef: kafka-rbac
  oidc:
    jwksEndpointUrl: http://keycloak.kafka.svc.clusterset.local:8080/realms/demo/protocol/openid-connect/certs
    expectedIssuer:  http://keycloak.kafka.svc.clusterset.local:8080/realms/demo
    expectedAudience: rbac-proxy
```

#### Custom filter

```yaml
spec:
  clusterRef: my-kafka
  poolRef: brokers
  image: kroxy-filters:dev
  customFilters:
    - name: my-filter
      type: com.example.MyFilterFactory
      config:
        someKey: someValue
```

---

## KafkaRbac

Declares topic-level and schema-registry access rules. One `KafkaRbac` CR generates two ConfigMaps: `{name}-kafka-rules` (consumed by Kroxylicious `GroupAwareAuthorizerService`) and `{name}-apicurio-policy` (consumed by the `apicurio-rbac-proxy`).

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `groups` | []KafkaRbacGroup | no | `[]` | Group-based rules. Matched against JWT role claims (typically `realm_access.roles` from Keycloak). |
| `users` | []KafkaRbacUser | no | `[]` | User-based rules. Matched against mTLS client-certificate CN. |

### spec.groups[] — KafkaRbacGroup

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Group name; matched against JWT role claims. |
| `kafka` | KafkaRbacKafkaAccess | no | Kafka topic rules. |
| `schemaRegistry` | KafkaRbacSchemaAccess | no | Apicurio artifact rules. |

### spec.users[] — KafkaRbacUser

For mTLS client-certificate CN authorization. No schema-registry rules — those are group-only.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Client certificate CN; matched against the mTLS Subject. |
| `kafka` | KafkaRbacKafkaAccess | no | Kafka topic rules. |
| `quotas` | KafkaQuotaConfig | no | Per-user Kafka client quotas (Wave 7 #21). Applied via `AdminClient.alterClientQuotas` on the primary cluster only. |

### KafkaRbacKafkaAccess

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topics` | []string | `[]` | Topic names this principal may access. `*` matches all topics. |
| `operations` | []string | `[]` | Allowed operations. Semantic aliases: `PRODUCE` (→ `WRITE` + `DESCRIBE`), `FETCH` (→ `READ` + `DESCRIBE`). Raw operations: `READ`, `WRITE`, `DESCRIBE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`. |

### KafkaQuotaConfig

All fields optional; unset means the operator does not push that quota (existing broker-side value, if any, is preserved).

| Field | Type | Description |
|-------|------|-------------|
| `producerByteRate` | int64 (Long) | Bytes/sec the user may produce. |
| `consumerByteRate` | int64 (Long) | Bytes/sec the user may consume. |
| `requestPercentage` | float64 (Double) | Fraction of broker IO/network threads the user may use (`0.5` = 50% of one thread). |
| `controllerMutationRate` | float64 (Double) | Controller mutations/sec (topic creates, ACL writes, ...). |

### KafkaRbacSchemaAccess

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `artifacts` | []string | `[]` | Apicurio artifact IDs this group may access. `*` matches all artifacts. |
| `actions` | []string | `[]` | Allowed actions: `READ`, `WRITE`, `DELETE`. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `RECONCILING` \| `READY` \| `FAILED` | Reconciliation state. |
| `message` | string | Status or error message. |

### Configuration recipes

#### Group-based (OIDC) — topics + schema registry

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
        artifacts: [orders, orders-value]
        actions: [READ, WRITE]
    - name: schema-admin
      schemaRegistry:
        artifacts: ["*"]
        actions: [READ, WRITE, DELETE]
```

#### User-based (mTLS CN)

```yaml
spec:
  users:
    - name: app1            # cert CN
      kafka:
        topics: [orders]
        operations: [PRODUCE]
    - name: read-only-app
      kafka:
        topics: ["*"]
        operations: [FETCH]
```

#### Combined — groups for OIDC clients, users for mTLS service accounts

```yaml
spec:
  groups:
    - name: invoices-team
      kafka:
        topics: [invoices]
        operations: [PRODUCE, FETCH]
  users:
    - name: invoices-batch
      kafka:
        topics: [invoices]
        operations: [WRITE, DESCRIBE]
```

---

## ApicurioRegistry

Manages an Apicurio Registry deployment and an optional HTTP RBAC proxy that enforces artifact-level access control using JWT roles from `spec.oidc`. When `rbacRef` is set, the operator mounts `{rbacRef}-apicurio-policy` into the proxy pod and hot-reloads it on change.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `quay.io/apicurio/apicurio-registry-kafkasql:latest-snapshot` | Apicurio Registry container image. Override when using a different storage backend (e.g. `apicurio-registry-mem` or `apicurio-registry-sql`). |
| `rbacProxyImage` | string | no | — | `apicurio-rbac-proxy` image. When set alongside `rbacRef`, the operator deploys `{name}-rbac-proxy`. |
| `rbacRef` | string | no | — | Name of a [`KafkaRbac`](#kafkarbac) CR. Determines which policy ConfigMap is mounted into the proxy. |
| `replicas` | integer | no | `1` | Registry pod count. |
| `oidc` | ApicurioRegistryOidcConfig | no | — | OIDC settings for the RBAC proxy. |
| `storage` | ApicurioRegistryStorageConfig | no | (defaults) | Storage backend selection. |
| `exportService` | boolean | no | `false` | When `true`, creates MCS `ServiceExport` resources for the registry and proxy Services. Redundant when `mcs.enabled=true` (which implies the export). |
| `externalAccess` | HttpExternalAccessConfig | no | — | When set, exposes the `{name}-rbac-proxy` Service externally. Requires `rbacRef` + `rbacProxyImage`; the raw registry on port 8080 is never exposed. See [shared HTTP external access](#http-external-access). |
| `mcs` | McsConfig | no | — | When `mcs.enabled=true`, the operator creates an MCS `ServiceExport` for the rbac-proxy Service so cross-cluster clients can resolve `<name>-rbac-proxy.<ns>.svc.clusterset.local`. Used with `targetClusters` for multi-cluster HA (#19). |
| `targetClusters` | []string | no | `[]` | List of cluster IDs (matching `KafkaCluster.spec.clusters[].id`) on which to deploy this registry. Operators on other clusters set `status.phase=SKIPPED`. Used in MCS mode where the same CR is applied to every cluster. |

### spec.oidc — ApicurioRegistryOidcConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `issuerUrl` | string | no | — | OIDC issuer base URL (e.g. `http://keycloak:8080/realms/demo`). |
| `groupsClaim` | string | no | `realm_access.roles` | JWT claim path for group/role list. Dots are converted to `/` for Quarkus OIDC config. |

### spec.storage — ApicurioRegistryStorageConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | no | `mem` | `mem` (in-memory, dev only) \| `postgresql` (Postgres-style backend) \| `kafkasql` (durable, Kafka-backed). |
| `jdbcUrl` | string | no | — | JDBC URL for the `postgresql` backend (e.g. `jdbc:postgresql://db.kafka.svc.cluster.local:5432/apicurio`). |
| `jdbcSecretRef` | string | no | — | Name of a Secret with JDBC credentials. The operator mounts it and exposes the credentials to the registry pod. |
| `clusterRef` | string | when `type=kafkasql` | — | Name of the [`KafkaCluster`](#kafkacluster) CR (same namespace) whose brokers will host the journal topic. |
| `kafkaTopic` | string | no | `kafkasql-journal` | Name of the compacted journal topic. The operator auto-creates a child `KafkaTopic` named `{registry}-kafkasql-journal`. |
| `tlsSecretRef` | string | when target cluster has `proxyMtls.enabled=true` | — | Cert-manager-style Secret (keys `tls.crt`, `tls.key`, `ca.crt`) holding the registry's client cert. Mounted read-only at `/etc/kafka/client-tls` and consumed via PEM keystore mode (no PKCS12). |
| `principal` | string | when target cluster has `proxyMtls.enabled=true` | — | CN inside `tlsSecretRef`. The reconciler provisions broker ACLs for `User:CN={principal}` covering the journal topic, consumer groups prefixed `apicurio-registry`, and `DESCRIBE` on the cluster. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `RECONCILING` \| `READY` \| `FAILED` \| `SKIPPED` | Deployment state. `SKIPPED` means this operator's cluster is not in `spec.targetClusters`. |
| `message` | string | Status or error message. |
| `proxyUrl` | string | ClusterIP URL of the RBAC proxy (`http://{name}-rbac-proxy.{namespace}.svc.cluster.local:8082`). Clients that need RBAC enforcement must use this URL with a Bearer JWT. The registry itself is reachable at `http://{name}-registry.{namespace}.svc.cluster.local:8080` for internal callers that bypass RBAC. |
| `externalUrl` | string | Externally-reachable URL when `externalAccess` is configured (LB IP, advertised host, etc.). Empty otherwise. |

### Configuration recipes

#### Dev (in-memory, no auth)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: ApicurioRegistry
metadata:
  name: apicurio
  namespace: kafka
spec:
  storage:
    type: mem
```

#### Production — SQL backend

```yaml
spec:
  image: quay.io/apicurio/apicurio-registry-sql:latest-snapshot
  replicas: 2
  storage:
    type: postgresql
    jdbcUrl: jdbc:postgresql://apicurio-db.kafka.svc.cluster.local:5432/apicurio
    jdbcSecretRef: apicurio-db-credentials   # keys: username, password
```

The secret must contain JDBC credentials in the form expected by the Apicurio image.

#### Production — `kafkasql` backend (durable, Kafka-backed)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: ApicurioRegistry
metadata:
  name: schema-registry
  namespace: kafka
spec:
  # image defaults to apicurio-registry-kafkasql:latest-snapshot
  replicas: 2
  storage:
    type: kafkasql
    clusterRef: my-kafka                 # KafkaCluster in same namespace
    kafkaTopic: kafkasql-journal         # default if omitted
    tlsSecretRef: schema-registry-client-tls   # cert-manager Secret
    principal: apicurio-registry         # CN inside the cert
```

The operator auto-creates a `KafkaTopic` CR named `schema-registry-kafkasql-journal`
(partitions=1, RF=3, `cleanup.policy=compact`, `min.insync.replicas=2`,
`deletionPolicy=RETAIN`) and provisions broker ACLs for `User:CN=apicurio-registry`
covering the journal topic, the `apicurio-registry`-prefixed consumer groups, and
`DESCRIBE` on the cluster. See `docs/architecture.md` for the full flow.

#### With OIDC RBAC proxy

```yaml
spec:
  image: quay.io/apicurio/apicurio-registry-mem:latest-snapshot
  rbacProxyImage: apicurio-rbac-proxy:dev
  rbacRef: kafka-rbac
  oidc:
    issuerUrl: http://keycloak.kafka.svc.cluster.local:8080/realms/demo
    groupsClaim: realm_access.roles
  storage:
    type: mem
```

`status.proxyUrl` is what clients should connect to; the bare registry Service still exists for callers that don't need RBAC enforcement.

#### MCS — export both Services across clusters

```yaml
spec:
  rbacProxyImage: apicurio-rbac-proxy:dev
  rbacRef: kafka-rbac
  oidc:
    issuerUrl: http://keycloak.kafka.svc.clusterset.local:8080/realms/demo
  storage:
    type: mem
  exportService: true
```

MCS `ServiceExport` is created for both the registry and the proxy, so cross-cluster clients resolve `apicurio-rbac-proxy.kafka.svc.clusterset.local` via the cluster fabric's MCS DNS.

---

## KafkaUI

Deploys the React + Quarkus **kafka-editor** as an operator-managed workload: ServiceAccount, namespaced Role/RoleBinding (read access to `KafkaCluster` / `KafkaRbac` CRs), Deployment, Service, and optional Ingress. The CRD shape is unchanged from the previous htmx-backed deployment — only the underlying image has flipped to `kafka-editor:dev`. The backend reads its bootstrap URL + OIDC config from operator-injected env vars (`KAFKA_EDITOR_BOOTSTRAP_SERVERS`, `KAFKA_EDITOR_TLS_DIR`, `QUARKUS_OIDC_*`) and surfaces the SPA at `/`; see [architecture.md → Kafka UI](architecture.md#kafka-ui-react--vite-spa--quarkus-317-backend).

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `kafka-editor:dev` | Container image. |
| `imagePullPolicy` | string | no | `IfNotPresent` | |
| `replicas` | int | no | `1` | |
| `oidc` | KafkaUIOidcConfig | **yes** | — | Keycloak SSO settings; the reconciler fails the CR if missing. |
| `tls` | KafkaUITlsConfig | no | `{ secretName: kafka-proxy-test-client-tls, mountPath: /etc/kafka-tls }` | Pre-provisioned client TLS Secret (cert-manager convention) mounted into the pod. |
| `discovery` | KafkaUIDiscoveryConfig | no | see below | Overrides for the env vars the UI uses to find proxy/Apicurio Services and which namespace to list `KafkaCluster` CRs from. |
| `resources` | ResourceRequirements | no | `requests: 100m/256Mi, limits: 500m/512Mi` | |
| `probes` | KafkaUIProbesConfig | no | `/q/health/ready` (5/5s) + `/q/health/live` (15/10s) | |
| `externalAccess` | HttpExternalAccessConfig | no | `{ type: NODEPORT }` | Same shape as `KafkaProxy.spec.externalAccess`. Pick `NODEPORT` / `LOADBALANCER` / `GATEWAY` / `INGRESS`. See [shared HTTP external access](#http-external-access). |
| `env[]` | KafkaUIEnvVar | no | `[]` | Extra env vars. A name collision overrides the operator-set default. |
| `mcs` | McsConfig | no | — | When `mcs.enabled=true`, the operator creates an MCS `ServiceExport` for the kafka-ui Service so cross-cluster clients can resolve `kafka-ui.<ns>.svc.clusterset.local`. Used with `targetClusters` for multi-cluster HA (#20). |
| `targetClusters` | []string | no | `[]` | List of cluster IDs (matching `KafkaCluster.spec.clusters[].id`) on which to deploy this UI. Operators on other clusters set `status.phase=SKIPPED`. Used in MCS mode where the same CR is applied to every cluster. |

### spec.oidc — KafkaUIOidcConfig

| Field | Type | Description |
|-------|------|-------------|
| `issuerUrl` | string | OIDC issuer (e.g. `http://keycloak.kafka.svc.clusterset.local:8080/realms/demo`). |
| `clientId` | string | Keycloak client ID. |
| `clientSecretRef.{name,key}` | SecretKeyRef | Secret holding the client secret. The operator does not create this Secret — apply it separately (see `kind/manifests/kafka-ui-oidc-secret.yaml`). |

### spec.discovery — KafkaUIDiscoveryConfig

| Field | Default | Description |
|-------|---------|-------------|
| `clusterNamespace` | `kafka` | Namespace the UI lists `KafkaCluster` CRs from. |
| `proxyServiceName` | `kafka-proxy` | |
| `proxyPort` | `9094` | |
| `apicurioServiceName` | `apicurio-rbac-proxy` | |
| `apicurioPort` | `8082` | |
| `dnsSuffix` | `""` | Empty for in-cluster (`svc.cluster.local`). Set to `clusterset.local` for MCS multi-cluster DNS (the MCS-spec namespace used by Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster, etc.). |

### status

| Field | Description |
|-------|-------------|
| `phase` | `RECONCILING` / `READY` / `FAILED` / `SKIPPED` (cluster not in `spec.targetClusters`) |
| `message` | Status or error message. |
| `readyReplicas` | From the underlying Deployment. |
| `observedGeneration` | Last `metadata.generation` the reconciler processed. |
| `advertisedHost` | Externally-reachable hostname/IP resolved on the local cluster (LB IP or substituted from `advertisedHostTemplate`). Empty for NodePort/internal. |

### Configuration recipes

#### Default — NodePort on kafka-a (matches the previous static manifest)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaUI
metadata:
  name: kafka-ui
  namespace: kafka
spec:
  oidc:
    issuerUrl: http://keycloak.kafka.svc.clusterset.local:8080/realms/demo
    clientId: kafka-ui-web
    clientSecretRef: { name: kafka-ui-oidc, key: client-secret }
```

Every other field is defaulted by the reconciler. Apply the OIDC client-secret Secret separately (`kafka-ui-oidc` is **not** operator-owned, so external Secret managers can supply it).

#### LoadBalancer (MetalLB / cloud)

```yaml
spec:
  externalAccess:
    type: LOADBALANCER
  oidc: { issuerUrl: ..., clientId: ..., clientSecretRef: { name: ..., key: ... } }
```

`status.advertisedHost` reports the assigned LB IP once the Service ingress is ready.

#### Ingress-fronted

```yaml
spec:
  externalAccess:
    type: INGRESS
    advertisedHostTemplate: "kafka-ui-${clusterId}.example.com"
    ingress:
      ingressClassName: nginx
      tlsSecretRef: kafka-ui-tls          # optional, BYO Secret for edge TLS termination
  oidc: { issuerUrl: ..., clientId: ..., clientSecretRef: { name: ..., key: ... } }
```

#### Gateway API HTTPRoute

```yaml
spec:
  externalAccess:
    type: GATEWAY
    advertisedHostTemplate: "kafka-ui.example.com"
    gateway:
      parentGatewayName: external-gw
      parentGatewayNamespace: gateway-system
      sectionName: https
  oidc: ...
```

Switching `externalAccess.type` is non-destructive: the reconciler creates the new edge resource and deletes the previous one on the next reconcile.

---

## KafkaTopic

Declarative Kafka topic. The CR's `metadata.name` is the Kafka topic name by default; set `spec.topicName` only when you need characters K8s names can't express (uppercase, underscore).

In MCS deployments where one `KafkaCluster` spans multiple K8s clusters, only the operator instance running on the primary cluster (`spec.clusters[0].id`) executes AdminClient writes. Other instances see the CR and set `status.phase=SKIPPED` with a message pointing at the primary — apply the CR identically on every cluster (e.g. via GitOps); only one instance acts.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | string | **yes** | — | Name of the `KafkaCluster` CR in the same namespace this topic belongs to. |
| `topicName` | string | no | `metadata.name` | Override the Kafka topic name. Must match `^[a-zA-Z0-9._-]{1,249}$`. |
| `partitions` | integer | no | `1` | Desired partition count. Increasing is supported; **decreasing is rejected** (Kafka doesn't support shrinking) and sets `status.phase=FAILED`. |
| `replicationFactor` | integer (short) | no | `1` | Desired replication factor. **Changing RF on an existing topic is rejected** (RF changes require partition reassignment, out of scope for the topic reconciler) and sets `status.phase=FAILED`. |
| `config` | map[string]string | no | `{}` | Dynamic topic-level config (e.g. `retention.ms`, `cleanup.policy`). Declarative: any key present on the topic but absent from `spec.config` is reset to broker default on reconcile. |
| `deletionPolicy` | `DELETE` \| `RETAIN` | no | `DELETE` | What happens to the Kafka topic when the CR is deleted. `RETAIN` leaves the topic in place. |

### status

| Field | Description |
|-------|-------------|
| `phase` | `RECONCILING`, `READY`, `SKIPPED` (peer cluster), or `FAILED`. |
| `message` | Human-readable detail for the current phase. |
| `observedGeneration` | `metadata.generation` last reconciled. |
| `topicId` | Kafka-assigned topic UUID. |
| `observedPartitions` | Partition count last observed on the broker. |
| `observedReplicationFactor` | Replication factor last observed on the broker. |
| `lastReconcileTime` | Timestamp of last reconciliation pass. |

### Reconcile cadence

The reconciler re-checks every 5 minutes to detect external drift (e.g. someone runs `kafka-configs.sh` directly). Edits to the CR fire reconciles immediately.

### Configuration recipes

#### Minimal (zero-config)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
  namespace: kafka
spec:
  clusterRef: my-kafka
  partitions: 3
  replicationFactor: 3
```

`metadata.name` is also the topic name. Default `deletionPolicy: DELETE` means deleting the CR removes the Kafka topic.

#### Retained data on CR deletion

```yaml
spec:
  clusterRef: my-kafka
  partitions: 6
  replicationFactor: 3
  config:
    cleanup.policy: compact
  deletionPolicy: RETAIN
```

`RETAIN` is appropriate for any topic whose data should survive a misclicked `kubectl delete`. The operator still removes its finalizer cleanly; only the Kafka topic stays.

#### Topic name override

```yaml
metadata:
  name: legacy-orders-v2
spec:
  clusterRef: my-kafka
  topicName: Legacy_Orders_V2   # uppercase + underscore not allowed in metadata.name
  partitions: 1
  replicationFactor: 3
```

### AdminClient transport

- When `KafkaCluster.spec.proxyMtls.enabled=false` (or absent), the reconciler connects plaintext to `<first-broker-pool>-headless.<ns>.svc.cluster.local:9092`.
- When `proxyMtls.enabled=true`, the reconciler connects with **mTLS using a PEM Secret** named by convention `kafka-operator-client-tls` (overridable via `KafkaCluster.spec.proxyMtls.adminClientCertSecretRef`). The Secret must follow the cert-manager convention: keys `tls.crt`, `tls.key` (PKCS#8 PEM), `ca.crt`. The cert's CN must be in broker `super.users` — by convention reuse `proxyPrincipal` (default `kafka-proxy`), so no broker config change is needed.
- If the Secret is missing when `proxyMtls=true`, the reconciler fails fast with a clear status message rather than blocking on an SSL-handshake-against-plaintext retry loop.

### Known limitations (v1)

- **Static primary cluster.** Failover requires reordering `spec.clusters` on the `KafkaCluster` CR. A future Kafka-consumer-group-based leader election will replace this without changing the CRD surface.
- **RF changes not driven.** Reassign partitions externally (or use Cruise Control once integrated) and the next reconcile will pick up the new state.

---

## MirrorMaker2

Cross-cluster (and cross-region) Kafka topic + schema replication driven by Apache Kafka's `connect-mirror-maker.sh`. The operator runs MM2 in **dedicated mode** — a single Deployment per CR hosting all replication connectors in one process group; workers form a Connect group via the internal topics on the target.

Each end (`spec.source` / `spec.target`) is independently either a **managed** reference to a `KafkaCluster` CR in this operator (bootstrap resolves to the cluster's Kroxylicious proxy, TLS material is reused from `proxyMtls`) or an **external** descriptor (raw bootstrap + optional TLS/SASL Secrets + optional schema registry). At least one end must be managed — there's nowhere for the operator to run MM2 otherwise.

A managed end is reached through the proxy, which enforces RBAC. The MM2 worker connects as the `proxyMtls.proxyPrincipal` identity, so that cluster's `KafkaRbac` must grant that principal broad Kafka access (`users: [{ name: <proxyPrincipal>, kafka: { topics: ["*"], operations: ["*"] } }]`) — MM2 mirrors arbitrary topics and manages its own internal topics. Without it the worker fails with `TopicAuthorizationException`.

When both ends carry an Apicurio schema registry (managed clusters with `spec.apicurio`, or external endpoints with `schemaRegistry` set) and `spec.schemaSync.enabled=true`, the operator wires an Apicurio-aware Connect SMT (`ApicurioSchemaTransferSmt`) into the MirrorSourceConnector. Per-record it parses the V3 envelope (`0x00` + 8-byte big-endian globalId), ensures the schema exists in the target registry (recursively for references), and rewrites the envelope with the target's globalId. See [Non-Apicurio topics](#non-apicurio-topics) below for the passthrough safety net.

The SMT **writes** mirrored schemas into the target registry. A managed cluster's Apicurio is reachable only through its `apicurio-rbac-proxy`, which OIDC-gates every request — writing into such a target needs `target.schemaRegistryAuthSecretRef`. See [Schema-registry authentication](#schema-registry-authentication).

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `mm2:dev` | MM2 worker image. The default `mm2:dev` image is built from `mm2-image/Dockerfile` (apache/kafka:4.0.0 + the schema-sync-smt JAR). |
| `imagePullPolicy` | string | no | `IfNotPresent` | Standard k8s pull policy. |
| `replicas` | integer | no | _derived_ | Worker count. When unset: 3 when the target is a multi-cluster managed KafkaCluster (MCS), otherwise 1. |
| `source` | [Mm2Endpoint](#mm2endpoint) | **yes** | — | The source end of the replication flow. |
| `target` | [Mm2Endpoint](#mm2endpoint) | **yes** | — | The target end of the replication flow. |
| `flow` | [Mm2FlowConfig](#mm2flowconfig) | no | — | Replication tuning (topic regexes, RF, tasks). |
| `schemaSync` | [Mm2SchemaSyncConfig](#mm2schemasyncconfig) | no | — | Schema-mirroring SMT config. When unset or `enabled=false`, MM2 mirrors topic data only. |
| `mcs.enabled` | bool | no | `false` | When true, the reconciler only runs on K8s clusters listed in `targetClusters`. |
| `targetClusters` | string[] | no | `[]` | K8s cluster IDs where this MM2 should be reconciled (MCS placement gate). |
| `clusterRollOrder` | string[] | no | — | Ordered cluster IDs for sequenced Deployment rolls on config/image change. |
| `metricsConfig` | [MetricsConfig](#specmetricsconfig--metricsconfig) | no | — | When set, exposes the MM2 Connect-worker JMX metrics via `jmx_prometheus_javaagent` and creates a `<name>-metrics` ClusterIP Service + ServiceMonitor on port 9101. The operator bundles a fixed JMX exporter config for Connect/MM2 — the `configMapRef` field is **not consulted** here; presence of `metricsConfig` alone enables metrics. |
| `resources` / `probes` | — | no | — | Same shapes as the KafkaUI resource/probes fields. **Set `resources` explicitly** — a real MM2 worker (3 connectors + clients + SMT) needs ~1.5Gi memory; the small default OOM-kills it. |

### Mm2Endpoint

Discriminated union — exactly one of `kafkaClusterRef` or `external` must be set (CEL-validated).

| Field | Type | Description |
|-------|------|-------------|
| `kafkaClusterRef.name` | string | Name of a `KafkaCluster` CR in the same namespace (cross-namespace not supported in v1). |
| `kafkaClusterRef.namespace` | string | Optional override (must match the MM2 CR's namespace in v1). |
| `external.bootstrap` | string | `host:port[,host:port,...]` for the external Kafka. |
| `external.tlsSecretRef` | string | PEM-shaped Secret (`tls.crt`/`tls.key`/`ca.crt`) for TLS or mTLS. |
| `external.sasl` | [Mm2SaslConfig](#mm2saslconfig) | Optional SASL credentials. |
| `external.schemaRegistry` | [Mm2SchemaRegistryRef](#mm2schemaregistryref) | Optional schema registry on this end. |
| `schemaRegistryAuthSecretRef` | string | OAuth2 client-credentials Secret authenticating the schema-sync SMT to this endpoint's registry. Required when the registry is behind an authenticating proxy (a managed cluster's `apicurio-rbac-proxy`). See [Schema-registry authentication](#schema-registry-authentication). |

### Mm2SaslConfig

| Field | Type | Description |
|-------|------|-------------|
| `mechanism` | string | One of `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`. |
| `secretRef` | string | Secret with `username` + `password` (or OAUTHBEARER token). |

### Mm2SchemaRegistryRef

| Field | Type | Description |
|-------|------|-------------|
| `url` | string | Base URL of the schema registry. |
| `type` | enum | `APICURIO` (v1) or `CONFLUENT` (reserved — rejected at reconcile in v1). |
| `authSecretRef` | string | Optional Secret with `username`/`password` or `token`. |

### Mm2FlowConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `flowName` | string | `metadata.name` | Suffix on internal topics (`mm2-{configs,offsets,status}.{flow}`). Distinct flow names let bi-directional MM2 CRs coexist. |
| `topics` / `topicsExclude` | string[] | `[".*"]` / `[]` | Topic regex allow/deny. |
| `groups` / `groupsExclude` | string[] | `[".*"]` / `[]` | Consumer group regex allow/deny (for checkpoint emission). |
| `replicationFactor` | integer | `3` | Target-side RF for mirrored topics + internal topics. |
| `syncTopicAcls` / `syncTopicConfigs` | bool | `false` / `true` | Mirror ACLs / topic configs from source. |
| `emitHeartbeats` | bool | `true` | Run the MirrorHeartbeatConnector. |
| `tasksMax` | integer | `4` | Connect `tasks.max`. Distributed across workers. |
| `additionalProperties` | map | `{}` | Passthrough escape hatch — keys are appended verbatim. |

### Mm2SchemaSyncConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch. |
| `cacheSize` | integer | `10000` | LRU cache size for source→target globalId mappings per worker. |
| `behaviorOnError` | enum | `WARN` | `FAIL` re-throws (worker dies); `WARN` logs + passes through; `IGNORE` drops. Default WARN for false-positive resilience. |
| `applyTo` | enum | `VALUE` | `VALUE`, `KEY`, or `BOTH`. |
| `applyToTopics` | string[] | `[".*"]` | Topic regex allowlist. Tighten in mixed-format clusters. |

### Schema-registry authentication

The schema-sync SMT reads from the source registry and **writes** mirrored schemas to the target registry. A managed cluster's Apicurio is reachable only through its `apicurio-rbac-proxy`, which OIDC-gates every request — so writing mirrored schemas into a managed target requires an authenticated identity with the `schema-admin` role.

Set `schemaRegistryAuthSecretRef` on the endpoint to a Secret holding OAuth2 client-credentials:

| Secret key | Description |
|------------|-------------|
| `token-url` | OAuth2 token endpoint (e.g. Keycloak `.../protocol/openid-connect/token`). |
| `client-id` | Client (service-account) id. |
| `client-secret` | Client secret. |
| `scope` | Optional OAuth scope. |

The SMT performs the `client_credentials` grant, caches the access token, and refreshes it before expiry (and on a 401/403) — a static bearer token would expire mid-run in a long-lived MM2 worker. The Secret is mounted into the worker at `/etc/mm2/registry-auth/{source,target}/`; the operator passes the SMT only the directory path, never the secret values. An unauthenticated registry (an external endpoint, or a bare Apicurio with no proxy) needs no `schemaRegistryAuthSecretRef`.

### status

| Field | Description |
|-------|-------------|
| `phase` | `RECONCILING` / `READY` / `PENDING` / `FAILED` / `SKIPPED`. PENDING means workers haven't come up yet. |
| `message` | Human-readable detail. |
| `observedGeneration` | `metadata.generation` last reconciled. |
| `readyReplicas` | Worker Deployment ready replicas. |
| `sourceBootstrap` / `targetBootstrap` | Resolved bootstrap URLs (the proxy address for managed refs). |
| `connectors[]` | Per-connector state (RUNNING/FAILED/PAUSED/UNASSIGNED) — populated from the `mm2-status.{flow}` topic. _v1: empty (placeholder)._ |
| `conditions[]` | Standard Kubernetes Condition list. |

### Non-Apicurio topics

The schema-sync SMT coexists with topics that don't use Apicurio. Six layers of safety:

1. **Per-record envelope detection.** Only acts when `bytes != null && bytes.length >= 9 && bytes[0] == 0x00`. Plain strings, JSON, XML, and BOM-prefixed text never trip this.
2. **Tombstones (null values)** pass through untouched.
3. **`applyTo`** keeps the SMT off the side you didn't intend to process (default `VALUE`).
4. **`applyToTopics`** allowlist skips non-Apicurio topics entirely.
5. **`behaviorOnError=WARN` (default)** swallows a source-404 (false positive) or target write failure and passes the record through. `FAIL` is available for strict environments.
6. **Per-record evaluation** — mixed-format topics (some records schema'd, some not) are handled per record.

Edge case: UTF-16BE-encoded XML without a BOM starts with `0x00 0x3C` — the envelope check fires, source registry returns 404, default behavior logs and passes through.

### Configuration recipes

#### Managed → managed with schema sync (DR within the same operator)

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: MirrorMaker2
metadata:
  name: prod-to-dr
  namespace: kafka
spec:
  source:
    kafkaClusterRef:
      name: prod
  target:
    kafkaClusterRef:
      name: dr
  flow:
    topics: ["events\\..*", "orders\\..*"]
    replicationFactor: 3
  schemaSync:
    enabled: true
```

#### Managed → external (Confluent Cloud, MSK, etc.)

```yaml
spec:
  source:
    kafkaClusterRef:
      name: my-kafka
  target:
    external:
      bootstrap: SASL_SSL_BROKER.aws.region.amazonaws.com:9098
      sasl:
        mechanism: SCRAM-SHA-512
        secretRef: msk-creds
```

`tasksMax` distributes across replicas (`tasks.max=4` on 3 replicas → 2/1/1 distribution).

### Internal topics

Three KafkaTopic CRs are created on the target managed cluster, owner-ref'd to the MM2 CR (cascade on delete):

| Topic | Partitions | RF | `cleanup.policy` |
|---|---|---|---|
| `mm2-configs.{flow}` | 1 | from `flow.replicationFactor` | `compact` |
| `mm2-offsets.{flow}` | 25 | from `flow.replicationFactor` | `compact` |
| `mm2-status.{flow}` | 5 | from `flow.replicationFactor` | `compact` |

When the target is external, these are skipped — the worker auto-creates on first start, or fails loudly if the external broker forbids auto-create.

### Known limitations (v1)

- **Confluent Schema Registry not supported.** `schemaRegistry.type=CONFLUENT` is reserved but rejected at reconcile time. Confluent's wire format uses a 4-byte schema ID, not Apicurio's 8-byte globalId.
- **No distributed Connect cluster mode.** Forward-compatible — a `spec.connectClusterRef` field will be added when the `KafkaConnect` CRD lands.
- **Per-connector status is a placeholder.** `status.connectors[]` is empty in v1; full status requires reading the `mm2-status.{flow}` topic via AdminClient. Phase = READY when all worker replicas are ready.
- **Worker client cert** reuses the operator's `kafka-operator-client-tls`. A future hardening pass will issue per-MM2-CR client certs via the per-pool cert pipeline.
- **Cross-namespace `kafkaClusterRef`** is rejected; managed source/target must live in the same namespace as the MM2 CR.

---

## HTTP external access

Shared sub-spec used by `KafkaUI.spec.externalAccess` and `ApicurioRegistry.spec.externalAccess`. (`KafkaProxy.spec.externalAccess` is similar but uses Gateway-API `TLSRoute` + nginx `ssl-passthrough` because the proxy terminates Kafka mTLS end-to-end — see [KafkaProxy](#kafkaproxy).)

### HttpExternalAccessConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | enum | `NODEPORT` | One of `NODEPORT` / `LOADBALANCER` / `GATEWAY` / `INGRESS`. |
| `advertisedHostTemplate` | string | — | Externally-reachable hostname. Required for `GATEWAY` and `INGRESS`. Optional for `LOADBALANCER` (overrides the auto-resolved LB IP). `${clusterId}` is substituted with the local cluster id (lowercase) so one CR yields different hosts per cluster in MCS. |
| `nodePort` | integer | — | Pins the NodePort. Only honoured when `type=NODEPORT`. |
| `gateway` | HttpGatewayConfig | — | Required when `type=GATEWAY`. |
| `ingress` | HttpIngressConfig | — | Required when `type=INGRESS`. |

### HttpGatewayConfig

| Field | Type | Description |
|-------|------|-------------|
| `parentGatewayName` | string | Name of the parent Gateway resource to attach the HTTPRoute to. |
| `parentGatewayNamespace` | string | Defaults to the CR's namespace. |
| `sectionName` | string | Optional Gateway listener section name. |
| `tlsSecretRef` | string | Informational — TLS termination happens on the Gateway listener; this records which Secret backs it. |

### HttpIngressConfig

| Field | Type | Description |
|-------|------|-------------|
| `ingressClassName` | string | Optional Ingress class. |
| `tlsSecretRef` | string | When set, a `spec.tls[]` entry is added so the Ingress controller terminates TLS using this Secret. |
| `annotations` | map[string]string | Extra annotations on the Ingress (e.g. cert-manager.io issuer hints). |

### Resource fan-out by type

| `type` | Service | Extra resource |
|--------|---------|----------------|
| `NODEPORT` | NodePort | — |
| `LOADBALANCER` | LoadBalancer | — |
| `GATEWAY` | ClusterIP | `gateway.networking.k8s.io/v1` HTTPRoute |
| `INGRESS` | ClusterIP | `networking.k8s.io/v1` Ingress |

Switching `type` is non-destructive: the reconciler creates the new edge resource on the next pass and deletes any stale Ingress/HTTPRoute.

---

## KafkaBackup

Scheduled, recurring backup of a managed `KafkaCluster`'s topic data — and, optionally, its
Apicurio schemas — to object storage. The operator does not implement a backup engine: it
renders a config for the open-source [osodevops/kafka-backup](https://github.com/osodevops/kafka-backup)
tool (MIT-licensed, compiled from source onto a UBI base in `kafka-backup-image/`) and builds
a Kubernetes **`CronJob`**. Kubernetes owns the schedule cadence; the operator re-renders the
CronJob on spec change and reflects run results into status.

Backup workloads connect **direct to the broker headless service** on the internal listener —
not through the Kroxylicious proxy — so bulk full-topic reads stay off the shared proxy. When
the cluster has `proxyMtls`, the broker mTLS PEM Secret is mounted and the tool connects over
TLS. Backups are written under `backup_id = metadata.name`.

> Topic **records and consumer-group offsets** are backed up. Topic **ACLs and dynamic
> configs are not** — rebuild those from your `KafkaTopic` / `KafkaRbac` CRs in git. See
> [disaster-recovery.md](disaster-recovery.md).

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `kafka-backup:dev` | kafka-backup tool image. |
| `imagePullPolicy` | string | no | `IfNotPresent` | Standard k8s pull policy. |
| `clusterRef` | KafkaClusterRef | **yes** | — | Managed `KafkaCluster` to back up (same namespace). |
| `placement.clusterId` | string | conditionally | — | Cluster the CronJob runs on. **Required** when the operator runs multi-cluster (MCS) — otherwise every operator instance would build a CronJob and the backup would run N times. Non-matching instances mark the CR `SKIPPED`. |
| `topics` | [BackupTopicSelector](#backuptopicselector) | no | all topics | Topic include/exclude patterns. |
| `storage` | [BackupStorageSpec](#backupstoragespec) | **yes** | — | Object-storage backend (exactly one of s3/azure/gcs/pvc). |
| `compression` | enum | no | `ZSTD` | `NONE` / `ZSTD` / `LZ4`. |
| `compressionLevel` | integer | no | tool default | Codec level. |
| `schedule` | string | **yes** | — | Cron schedule for the CronJob. |
| `concurrencyPolicy` | string | no | `Forbid` | CronJob concurrency: `Allow` / `Forbid` / `Replace`. |
| `suspend` | bool | no | `false` | Pause the schedule without deleting the CR. |
| `startingDeadlineSeconds` | integer | no | `300` | Missed-schedule catch-up bound. |
| `successfulJobsHistoryLimit` / `failedJobsHistoryLimit` | integer | no | `3` / `3` | Job history retained. |
| `includeSchemas` | bool | no | _auto_ | Export Apicurio schemas with each backup. Auto = true when the cluster has an Apicurio sub-spec. Supported for `pvc` and `s3` storage only. |
| `schemaRegistryAuthSecretRef` | string | no | — | OAuth2 client-credentials Secret (`token-url`, `client-id`, `client-secret`) for the schema export when Apicurio is behind an authenticating proxy. |
| `activeDeadlineSeconds` | integer | no | `3600` | Per-run hard timeout. |
| `resources` | KafkaUIResourceRequirements | no | small | CPU/memory requests + limits. |
| `additionalConfig` | map[string]string | no | `{}` | Extra keys merged verbatim into the rendered `backup:` config section. |

### BackupStorageSpec

Discriminated union — exactly one of `s3` / `azure` / `gcs` / `pvc` (CEL-validated). Cloud
credentials come from a referenced Secret and are mounted as env vars — never inlined.

| Sub-field | Keys | Credentials Secret keys |
|-----------|------|--------------------------|
| `s3` | `bucket`*, `prefix`, `region`, `endpoint` (MinIO), `pathStyleAccess`, `credentialsSecretRef`* | `accessKeyId`, `secretAccessKey` |
| `azure` | `container`*, `prefix`, `credentialsSecretRef`* | `accountName`, `accountKey` |
| `gcs` | `bucket`*, `prefix`, `credentialsSecretRef`* | `key.json` |
| `pvc` | `claimName`*, `subPath` | — (filesystem) |

### BackupTopicSelector

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include` | string[] | `["*"]` | Topic name patterns to back up. |
| `exclude` | string[] | `["__consumer_offsets", "_schemas"]` | Patterns to skip. |

### status

| Field | Description |
|-------|-------------|
| `phase` | `RECONCILING` / `SCHEDULED` / `SUSPENDED` / `SKIPPED` / `FAILED`. |
| `cronJobName`, `schedule`, `resolvedBootstrap` | The created CronJob, its schedule, the resolved broker bootstrap. |
| `lastScheduleTime`, `lastSuccessfulBackupTime` | From the CronJob status. |
| `lastJobName`, `lastJobResult` | Most recent child Job and its result (`SUCCEEDED` / `FAILED` / `RUNNING` / `UNKNOWN`). |
| `activeBackupCount` | Currently-running backup Jobs. |

---

## KafkaRestore

One-shot restore of a `KafkaBackup` into a managed `KafkaCluster`. The operator builds a
Kubernetes **`Job`** exactly once. The reconciler is **idempotent**: once `status.phase` is
terminal it never re-creates the Job — re-running a restore requires deleting and re-creating
the CR.

Restore is destructive, so `spec.confirm` must be `true` or the reconciler refuses. Before the
Job is created, the reconciler runs an AdminClient pre-flight check of the literal target
topics against `spec.targetPolicy`. When `restoreSchemas` is set, Apicurio schemas are imported
(as an init container) before records land.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` / `imagePullPolicy` | string | no | `kafka-backup:dev` / `IfNotPresent` | kafka-backup tool image. |
| `targetClusterRef` | KafkaClusterRef | **yes** | — | Managed cluster to restore into. |
| `placement.clusterId` | string | conditionally | — | Cluster the restore Job runs on (required multi-cluster). |
| `source` | RestoreSourceSpec | **yes** | — | Exactly one of `kafkaBackupRef` (a KafkaBackup CR name) or inline `storage`. |
| `backupId` | string | no | _kafkaBackupRef name_ | Specific backup id. Required when `source.storage` is inline. |
| `topics` | BackupTopicSelector | no | all | Topics to restore. |
| `topicMapping` | map[string]string | no | `{}` | Source→target topic renaming (restore into non-live topics). |
| `timeWindow` | RestoreTimeWindow | no | — | Point-in-time-recovery: `startMillis` / `endMillis` (epoch ms). |
| `restoreOffsets` | bool | no | `false` | Restore committed consumer-group offsets. Dangerous against live groups. |
| `consumerGroups` | string[] | no | `[]` | Groups whose offsets to restore (with `restoreOffsets`). |
| `targetPolicy` | enum | no | `REQUIRE_EMPTY` | Pre-flight guard: `REQUIRE_ABSENT` / `REQUIRE_EMPTY` / `ALLOW_NON_EMPTY`. Checks literal topic names only. |
| `confirm` | bool | **yes (true)** | `false` | Must be `true` — acknowledges the restore is destructive. |
| `restoreSchemas` | bool | no | _auto_ | Import Apicurio schemas before records. |
| `createTopics` | bool | no | `true` | Create absent target topics. |
| `dryRun` | bool | no | `false` | Report what would be restored without writing. |
| `activeDeadlineSeconds` | integer | no | `7200` | Hard timeout. |

### status

`phase` walks `PENDING` → `RUNNING` → `SUCCEEDED` / `FAILED` (or `SKIPPED`). `jobName`,
`resolvedBootstrap`, `startTime`, `completionTime` track the Job.

---

## KafkaBackupValidation

One-shot integrity check of a stored backup. The operator builds a `Job` that runs
`kafka-backup validate`; the terminal phase reflects whether the backup is restorable.
Idempotent like KafkaRestore.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` / `imagePullPolicy` | string | no | `kafka-backup:dev` / `IfNotPresent` | kafka-backup tool image. |
| `placement.clusterId` | string | conditionally | — | Cluster the validation Job runs on (required multi-cluster). |
| `source` | RestoreSourceSpec | **yes** | — | Exactly one of `kafkaBackupRef` or inline `storage`. |
| `backupId` | string | no | _kafkaBackupRef name_ | Specific backup id. |
| `reportFormat` | enum | no | `JSON` | `JSON` / `PDF`. |
| `deep` | bool | no | `true` | Deep validation reads all data and verifies checksums; quick checks segment metadata only. |
| `activeDeadlineSeconds` | integer | no | `3600` | Hard timeout. |

### status

`phase` walks `PENDING` → `RUNNING` → `VALID` / `INVALID` / `FAILED` (or `SKIPPED`).

---

## KafkaRebalance

Declarative Cruise Control rebalance. The operator generates an optimization proposal, surfaces it on `status.optimizationResult`, and — once the user approves it via an annotation — executes the rebalance and tracks it to completion. Requires the referenced `KafkaCluster` to have `spec.cruiseControl` enabled. Reconciled only by the operator on the primary cluster (`spec.clusters[0]`).

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | string | **yes** | — | Name of the target `KafkaCluster` in the same namespace. |
| `mode` | enum | no | `full` | `full` (whole-cluster rebalance), `add-brokers`, or `remove-brokers`. |
| `brokers` | []integer | conditional | — | Broker IDs to add/remove. Required for `add-brokers` / `remove-brokers`. |
| `goals` | []string | no | — | Cruise Control goal class names to optimize against. Empty = Cruise Control defaults. |
| `skipHardGoalCheck` | boolean | no | `false` | Skip the check that the proposal satisfies all hard goals. |
| `rebalanceDisk` | boolean | no | `false` | Intra-broker disk rebalance (across log dirs). Only honored for `mode: full`. |
| `concurrentPartitionMovementsPerBroker` | integer | no | — | Cap on concurrent partition movements per broker. |
| `concurrentLeaderMovements` | integer | no | — | Cap on concurrent leadership movements. |
| `replicationThrottle` | long | no | — | Replication throttle in bytes/sec applied during execution. |
| `excludedTopics` | string | no | — | Regex of topic names to exclude from replica movement. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | enum | `NEW` → `PENDING_PROPOSAL` → `PROPOSAL_READY` → `REBALANCING` → `READY`; plus `STOPPED` and `NOT_READY`. |
| `message` | string | Human-readable status / error message. |
| `optimizationResult` | map[string]string | Flattened Cruise Control proposal summary (data to move, # movements, balancedness scores). |
| `dataToMoveMB` | string | Denormalized `dataToMoveMB` from the proposal (printer column). |
| `sessionId` | string | Cruise Control User-Task-ID of the proposal request. |
| `executionTaskId` | string | Cruise Control User-Task-ID of the execution request. |
| `conditions` | []Condition | Standard `Ready` condition. |
| `observedGeneration` | long | Generation of the spec last reconciled. |

### Approval annotation

The CR is driven by the annotation `kafka.yavari.afshin.se/rebalance`. The operator consumes and clears it.

| Value | Effect |
|-------|--------|
| `approve` | Execute the ready proposal (`PROPOSAL_READY` → `REBALANCING`). |
| `refresh` | Discard the current proposal and regenerate it (→ `NEW`). |
| `stop` | Abort — discard the proposal or stop an in-flight execution (→ `STOPPED`). |

A spec edit on a settled CR also regenerates the proposal from scratch.

### Configuration recipes

#### Full cluster rebalance

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRebalance
metadata:
  name: nightly-rebalance
  namespace: kafka
spec:
  clusterRef: my-kafka
  mode: full
```

Then, once `status.phase` is `PROPOSAL_READY`:

```bash
kubectl annotate kafkarebalance nightly-rebalance \
  kafka.yavari.afshin.se/rebalance=approve
```

#### Add brokers

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRebalance
metadata:
  name: scale-out
  namespace: kafka
spec:
  clusterRef: my-kafka
  mode: add-brokers
  brokers: [1003, 1004]
  replicationThrottle: 10485760
```

#### Intra-broker disk rebalance

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRebalance
metadata:
  name: disk-rebalance
  namespace: kafka
spec:
  clusterRef: my-kafka
  mode: full
  rebalanceDisk: true
```
