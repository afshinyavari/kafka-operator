# API Reference

All CRDs are in group `kafka.yavari.afshin.se`, version `v1alpha1`.

---

## KafkaCluster

Cluster-scoped configuration and KRaft quorum definition. One `KafkaCluster` CR is deployed per cluster; all clusters in the quorum must carry the same `spec.clusters` list.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `kafkaImage` | string | no | `kafka-ubi:4.0.0` | Container image for all Kafka pods |
| `kafkaVersion` | string | no | `"4.0"` | Kafka version string; used for upgrade tracking and downgrade protection |
| `targetMetadataVersion` | integer | no | — | When set, operator bumps `metadata.version` to this value after all pods are on `kafkaVersion`. Must not be lower than `status.currentMetadataVersion`. |
| `clusters` | []ClusterEntry | **yes** | — | Ordered list of all clusters in the KRaft quorum. Index position determines controller `node.id` (`10000 + index`). At least 1 entry required. |
| `config` | map[string]string | no | `{}` | Shared Kafka config properties applied to all node pools. Operator-owned keys (see below) are silently overridden. |
| `metricsConfig` | MetricsConfig | no | — | Enables JMX metrics. When present, a `jmx_prometheus_javaagent` is started in each broker pod. |
| `clusterRollOrder` | []string | no | — | Ordered list of cluster IDs (matching `spec.clusters[].id`) for cross-cluster rolling coordination. First cluster in the list rolls first. Absent = no coordination. |
| `listeners` | []KafkaListenerSpec | no | `[]` | Additional client-facing listeners beyond the always-present `INTERNAL:9092`. When non-empty, `INTERNAL` binds to `127.0.0.1` if any internal TLS listener is present. |
| `controllerTls` | KafkaListenerTlsConfig | no | — | Enables TLS on the KRaft `CONTROLLER:9093` listener. When set, all nodes load their TLS secret at startup. |

### spec.clusters[] — ClusterEntry

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | **yes** | Logical cluster ID. Must match the `KAFKA_CLUSTER_ID` env var on that cluster's operator deployment. |
| `controllerAdvertisedAddress` | string | **yes** | `host:port` for the controller's quorum listener. Use `{pool}-headless.kafka.svc.clusterset.local:9093` in MCS mode. |
| `operatorAddress` | string | no | `host:port` for the operator's HTTP endpoint. Required when `clusterRollOrder` is set; used for cross-cluster upgrade-phase polling. |

### spec.metricsConfig — MetricsConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `configMapRef` | string | **yes** | Name of a `ConfigMap` in the same namespace with a `jmx-config.yaml` key containing the JMX exporter config. |

### spec.listeners[] — KafkaListenerSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | **yes** | — | Listener name, uppercase alphanumeric + underscores (e.g. `CLIENT_TLS`). Must not be `INTERNAL` or `CONTROLLER`. |
| `port` | integer | **yes** | — | Port number. Must not be 9092 or 9093. |
| `tls` | KafkaListenerTlsConfig | no | — | When present, listener uses SSL protocol and PKCS12 keystores. |
| `externalAccess` | ExternalAccessType | no | — | `null` = internal-only (bound to `0.0.0.0`). `NODEPORT` = operator creates one NodePort Service per broker pod. `LOADBALANCER` = reserved, not yet implemented. |
| `nodePortBase` | integer | no | `31000` | Base nodePort for `NODEPORT` type. Broker at ordinal `N` gets `nodePortBase + N`. Must be in the Kubernetes NodePort range (30000–32767). |

### spec.controllerTls / spec.listeners[].tls — KafkaListenerTlsConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mutualTls` | boolean | no | `false` | When `true`, sets `client.auth=required` — clients must present a certificate signed by the CA in the TLS secret. |

TLS requires a Secret named `{podName}-tls` in the same namespace with keys `tls.crt`, `tls.key`, and `ca.crt` (cert-manager convention). The operator converts them to PKCS12 keystores at pod startup.

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | RECONCILING \| READY \| DEGRADED \| FAILED | Overall cluster state |
| `message` | string | Human-readable status message |
| `lastReconcileTime` | string | ISO-8601 timestamp of last reconcile |
| `observedGeneration` | long | Generation of the spec last reconciled |
| `poolPhases` | map[string]string | Per-pool readiness: `"ready/desired"` (e.g. `"3/3"`) |
| `currentKafkaVersion` | string | Kafka version currently running |
| `upgradePhase` | string | `IDLE` \| `ROLLING` — used by cross-cluster roll coordinator |
| `currentMetadataVersion` | integer | `metadata.version` currently active in the cluster |

### Operator-owned config keys

The following keys in `spec.config` are **silently overridden** by the operator and should not be set by the user:

`process.roles`, `node.id`, `cluster.id`, `controller.quorum.voters`, `listeners`, `advertised.listeners`, `listener.security.protocol.map`, `controller.listener.names`, `log.dirs`, `inter.broker.listener.name`, `controller.advertised.listeners`, and all `listener.name.*` SSL properties.

---

## KafkaNodePool

Defines a set of Kafka nodes with a common role, replica count, and resource configuration. Must carry the label `kafka.yavari.afshin.se/cluster={clusterName}`.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `roles` | []NodeRole | **yes** | — | At least one of `CONTROLLER`, `BROKER`. Both may be set for combined nodes. |
| `replicas` | integer | **yes** | `1` | Number of pods. Minimum 1. |
| `storage` | StorageSpec | no | 10Gi | PVC spec for Kafka data (`/var/lib/kafka/data`). PVCs are retained on pod deletion and scale-down. |
| `resources` | ResourceRequirements | no | — | Standard Kubernetes `resources` block (requests + limits for CPU and memory). |
| `config` | map[string]string | no | `{}` | Pool-level Kafka config overrides. Merged over `KafkaCluster.spec.config`; same operator-owned key restrictions apply. |
| `rackTopologyKey` | string | no | — | Node label key for `broker.rack` assignment and topology spread (e.g. `topology.kubernetes.io/zone`). When set, the operator reads node labels and assigns zones deterministically. |

### spec.storage — StorageSpec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `size` | string | no | `"10Gi"` | PVC storage request (Kubernetes quantity string). |
| `storageClassName` | string | no | — | Storage class name. Omit to use the cluster default. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | PENDING \| RECONCILING \| READY \| FAILED | Pool state |
| `message` | string | Status message |
| `readyReplicas` | integer | Number of ready pods |
| `desiredReplicas` | integer | Target replica count from spec |
| `lastError` | string | Last reconciliation error, if any |

---

## KafkaPodSet

Operator-managed. Do not create or modify directly. One `KafkaPodSet` is created per `KafkaNodePool`; its name is `{poolName}-podset`.

### Useful status fields for debugging

| Field | Description |
|-------|-------------|
| `status.currentRollingPod` | Name of the pod currently being rolled. Empty string when no roll is in progress. If this is non-empty and not changing, a roll may be blocked by an ISR/quorum check. |
| `status.pods[].specHash` | Desired spec hash for this pod. |
| `status.pods[].currentSpecHash` | Hash of the running pod's spec. If these differ, a rolling update is pending or in progress. |
| `status.pods[].ready` | Whether the pod currently passes its readiness probe. |

---

## Listener Configuration Examples

### Default — INTERNAL only (plaintext, intra-cluster)

No `spec.listeners` needed. Each broker advertises `{pod}.{pool}-headless.{ns}.svc.cluster.local:9092`.

```yaml
spec:
  # No listeners field required
```

### Internal TLS listener (for encrypted inter-broker + client traffic)

```yaml
spec:
  listeners:
    - name: CLIENT_TLS
      port: 9094
      tls:
        mutualTls: false
```

When `listeners` contains a TLS entry, `INTERNAL` binds to `127.0.0.1` only (admin tools within the pod), and `CLIENT_TLS` becomes the `inter.broker.listener.name`. Each broker pod requires a Secret named `{podName}-tls`.

### External NodePort (plaintext, for clients outside the cluster)

```yaml
spec:
  listeners:
    - name: EXTERNAL
      port: 9094
      externalAccess: NODEPORT
      nodePortBase: 31000
```

Produces services `{pool}-0-external-ext` (nodePort 31000), `{pool}-1-external-ext` (31001), etc. Each broker advertises `{nodeIP}:{nodePort}`. `INTERNAL` remains the inter-broker listener (binds `0.0.0.0`).

### Both internal TLS and external NodePort

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

`CLIENT_TLS` is the inter-broker listener; `INTERNAL` is localhost-only for admin tools. `EXTERNAL` is advertised via node IP + NodePort.

---

## KafkaProxy

Deploys a Kroxylicious proxy in front of a broker pool. Clients connect to the proxy port
(`spec.clientPort`) instead of directly to Kafka. The proxy enforces SASL/OAUTHBEARER auth
and optionally validates message payloads. One `KafkaProxy` per cluster is the typical setup.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `clusterRef` | string | **yes** | — | Name of the `KafkaCluster` CR |
| `poolRef` | string | **yes** | — | Name of the `KafkaNodePool` (broker pool) this proxy fronts |
| `replicas` | integer | no | `1` | Number of proxy pods |
| `image` | string | no | `kroxy-filters:dev` | Kroxylicious container image with custom filters |
| `clientPort` | integer | no | `9094` | Port exposed to Kafka clients |
| `rbacRef` | string | no | — | Name of a `KafkaRbac` CR. When set, `GroupAwareAuthorizerService` is activated and topic ACLs are enforced. |
| `apicurioRef` | string | no | — | Name of an `ApicurioRegistry` CR. When set and the registry is ready, `record-validation` filter is added. |
| `brokerNodeIdRanges[]` | []BrokerNodeIdRange | no | — | Explicit mapping of node ID ranges to pool names. When omitted, the operator infers ranges by inspecting pod labels. |
| `oidc` | KafkaProxyOidcConfig | no | — | Enables SASL/OAUTHBEARER + JWT group extraction. When set, the four-filter OIDC chain is injected. |
| `filters` | KafkaProxyFiltersConfig | no | — | Built-in filter toggles (XML validation, schema-registry payload validation). |
| `customFilters[]` | []KafkaProxyCustomFilter | no | — | Arbitrary Kroxylicious filter entries appended verbatim to `config.yaml`. |

### spec.oidc — KafkaProxyOidcConfig

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `jwksEndpointUrl` | string | **yes** | JWKS URL for JWT signature verification (e.g. Keycloak `/certs` endpoint) |
| `groupsClaim` | string | no | Dot-separated JWT claim path containing the group/role list (e.g. `realm_access.roles`) |
| `expectedIssuer` | string | no | If set, the `iss` claim must match this value |
| `expectedAudience` | string | no | If set, the `aud` claim must contain this value |

### spec.filters — KafkaProxyFiltersConfig

| Field | Type | Description |
|-------|------|-------------|
| `xmlValidation.enabled` | boolean | Enables XML schema validation filter. Schemas are read from a Kafka topic. |
| `xmlValidation.schemaTopic` | string | Kafka topic name where XML schemas are stored |
| `schemaRegistry.enabled` | boolean | Enables `RecordValidation` filter for JSON/Avro/Protobuf schema checking via Apicurio |
| `schemaRegistry.schemaType` | string | `JSON_SCHEMA`, `AVRO`, or `PROTOBUF` |
| `schemaRegistry.topics[]` | []string | Topic names to apply schema validation to |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | RECONCILING \| READY \| FAILED | Proxy deployment state |
| `message` | string | Status message or error detail |
| `readyReplicas` | integer | Number of ready proxy pods |

---

## KafkaRbac

Declares topic-level and schema-registry access rules. One `KafkaRbac` CR generates two
ConfigMaps: `{name}-kafka-rules` (consumed by Kroxylicious `GroupAwareAuthorizerService`)
and `{name}-apicurio-policy` (consumed by the `apicurio-rbac-proxy`).

### spec.groups[] — KafkaRbacGroup

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Group name, matched against JWT role claims (e.g. Keycloak realm roles) |
| `kafka.topics` | []string | no | Topic names this group may access. `*` matches all topics. |
| `kafka.operations` | []string | no | Allowed operations. Semantic names: `PRODUCE` (→ WRITE + DESCRIBE), `FETCH` (→ READ + DESCRIBE). Raw names: `READ`, `WRITE`, `DESCRIBE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`. |
| `schemaRegistry.artifacts` | []string | no | Apicurio artifact IDs this group may access. `*` matches all artifacts. |
| `schemaRegistry.actions` | []string | no | `READ`, `WRITE`, `DELETE` |

### spec.users[] — KafkaRbacUser

Used for mTLS client certificate CN-based authorization (no schema registry support).

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **yes** | Client certificate CN; matched against the mTLS Subject |
| `kafka.topics` | []string | no | Allowed topic names |
| `kafka.operations` | []string | no | Allowed operations (same values as group operations above) |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | RECONCILING \| READY \| FAILED | Reconciliation state |
| `message` | string | Status or error message |

---

## ApicurioRegistry

Manages an Apicurio Registry deployment and an optional HTTP RBAC proxy that enforces
artifact-level access control using JWT roles from `spec.oidc`. When `rbacRef` is set, the
operator mounts `{rbacRef}-apicurio-policy` into the proxy pod and hot-reloads it on change.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `quay.io/apicurio/apicurio-registry-mem:latest-snapshot` | Apicurio Registry container image |
| `rbacProxyImage` | string | no | — | `apicurio-rbac-proxy` image. When set alongside `rbacRef`, the operator deploys `{name}-rbac-proxy`. |
| `rbacRef` | string | no | — | Name of a `KafkaRbac` CR. Determines which policy ConfigMap is mounted into the proxy. |
| `replicas` | integer | no | `1` | Registry pod count |
| `storage.type` | string | no | `mem` | Storage backend: `mem` (in-memory, dev only) or `sql` |
| `oidc.issuerUrl` | string | no | — | OIDC issuer base URL for the RBAC proxy (e.g. `http://keycloak:8080/realms/demo`) |
| `oidc.groupsClaim` | string | no | `realm_access.roles` | JWT claim path for group/role list. Dots are converted to `/` for Quarkus OIDC config. |
| `exportService` | boolean | no | `false` | When `true`, creates `ServiceExport` resources for both registry and proxy services (MCS mode) |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | RECONCILING \| READY \| FAILED | Deployment state |
| `message` | string | Status or error message |
| `registryUrl` | string | ClusterIP URL of the registry (`http://{name}-registry.{namespace}.svc.cluster.local:8080`). Use this for internal service-to-service access that bypasses RBAC. |
| `proxyUrl` | string | ClusterIP URL of the RBAC proxy (`http://{name}-rbac-proxy.{namespace}.svc.cluster.local:8082`). Clients that need RBAC enforcement must use this URL with a Bearer JWT. |
