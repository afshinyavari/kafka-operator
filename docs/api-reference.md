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
| `metricsConfig` | MetricsConfig | no | — | Enables JMX metrics. When present, `jmx_prometheus_javaagent` is started in each broker pod. |
| `clusterRollOrder` | []string | no | — | Ordered list of cluster IDs (matching `spec.clusters[].id`) for cross-cluster rolling coordination. First cluster in the list rolls first. Absent = no coordination. |
| `listeners` | []KafkaListenerSpec | no | `[]` | Additional client-facing listeners beyond the always-present `INTERNAL:9092`. When non-empty, `INTERNAL` binds to `127.0.0.1` only and the first entry becomes `inter.broker.listener.name`. |
| `controllerTls` | KafkaListenerTlsConfig | no | — | Enables TLS on the KRaft `CONTROLLER:9093` listener. When set, all nodes load their TLS secret at startup. |
| `proxyMtls` | KafkaProxyMtlsConfig | no | — | Enables mTLS on the broker `INTERNAL` listener for a Kroxylicious-style proxy. Driven from the cluster spec (not KafkaProxy presence) so every cluster in the MCS topology reconciles consistently, even when the proxy Deployment only runs on one cluster. |

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
| `mcs` | KafkaProxyMcsConfig | no | — | Enables MCS (Submariner ServiceExport) mode for cross-cluster client resolution. |
| `targetClusters` | []string | no | `[]` | List of cluster IDs (matching `KafkaCluster.spec.clusters[].id`) on which to deploy this proxy. Operators on other clusters set `status.phase=SKIPPED`. Used in MCS mode where the same CR is applied to every cluster. |
| `filters` | KafkaProxyFiltersConfig | no | (defaults) | Built-in filter toggles (XML validation, schema-registry payload validation). |
| `customFilters` | []KafkaProxyCustomFilter | no | `[]` | Arbitrary Kroxylicious filter entries appended verbatim to `config.yaml`. |

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

### spec.mcs — KafkaProxyMcsConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | no | `false` | When `true`, the operator creates a Submariner `ServiceExport` for the proxy Service so cross-cluster clients resolve `kafka-proxy.<ns>.svc.clusterset.local`. Used with `targetClusters`. |

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

### KafkaRbacKafkaAccess

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topics` | []string | `[]` | Topic names this principal may access. `*` matches all topics. |
| `operations` | []string | `[]` | Allowed operations. Semantic aliases: `PRODUCE` (→ `WRITE` + `DESCRIBE`), `FETCH` (→ `READ` + `DESCRIBE`). Raw operations: `READ`, `WRITE`, `DESCRIBE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE_CONFIGS`, `ALTER_CONFIGS`. |

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
| `image` | string | no | `quay.io/apicurio/apicurio-registry-mem:latest-snapshot` | Apicurio Registry container image. |
| `rbacProxyImage` | string | no | — | `apicurio-rbac-proxy` image. When set alongside `rbacRef`, the operator deploys `{name}-rbac-proxy`. |
| `rbacRef` | string | no | — | Name of a [`KafkaRbac`](#kafkarbac) CR. Determines which policy ConfigMap is mounted into the proxy. |
| `replicas` | integer | no | `1` | Registry pod count. |
| `oidc` | ApicurioRegistryOidcConfig | no | — | OIDC settings for the RBAC proxy. |
| `storage` | ApicurioRegistryStorageConfig | no | (defaults) | Storage backend selection. |
| `exportService` | boolean | no | `false` | When `true`, creates Submariner `ServiceExport` resources for the registry and proxy Services (MCS mode). |

### spec.oidc — ApicurioRegistryOidcConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `issuerUrl` | string | no | — | OIDC issuer base URL (e.g. `http://keycloak:8080/realms/demo`). |
| `groupsClaim` | string | no | `realm_access.roles` | JWT claim path for group/role list. Dots are converted to `/` for Quarkus OIDC config. |

### spec.storage — ApicurioRegistryStorageConfig

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | no | `mem` | `mem` (in-memory, dev only) \| `sql` (Postgres-style backend). |
| `jdbcUrl` | string | no | — | JDBC URL for the `sql` backend (e.g. `jdbc:postgresql://db.kafka.svc.cluster.local:5432/apicurio`). |
| `jdbcSecretRef` | string | no | — | Name of a Secret with JDBC credentials. The operator mounts it and exposes the credentials to the registry pod. |

### status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | `RECONCILING` \| `READY` \| `FAILED` | Deployment state. |
| `message` | string | Status or error message. |
| `proxyUrl` | string | ClusterIP URL of the RBAC proxy (`http://{name}-rbac-proxy.{namespace}.svc.cluster.local:8082`). Clients that need RBAC enforcement must use this URL with a Bearer JWT. The registry itself is reachable at `http://{name}-registry.{namespace}.svc.cluster.local:8080` for internal callers that bypass RBAC. |

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
  replicas: 2
  storage:
    type: sql
    jdbcUrl: jdbc:postgresql://apicurio-db.kafka.svc.cluster.local:5432/apicurio
    jdbcSecretRef: apicurio-db-credentials   # keys: username, password
```

The secret must contain JDBC credentials in the form expected by the Apicurio image.

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

Submariner `ServiceExport` is created for both the registry and the proxy, so cross-cluster clients resolve `apicurio-rbac-proxy.kafka.svc.clusterset.local`.

---

## KafkaUI

Deploys the Quarkus + htmx Kafka UI as an operator-managed workload: ServiceAccount, namespaced Role/RoleBinding (read access to `KafkaCluster`/`KafkaProxy`/`KafkaRbac`/`ApicurioRegistry` CRs), Deployment, Service, and optional Ingress. The UI app itself discovers `KafkaCluster` CRs at runtime via the K8s API; it does not reference one through the CRD.

### spec

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `image` | string | no | `kafka-ui:dev` | Container image. |
| `imagePullPolicy` | string | no | `IfNotPresent` | |
| `replicas` | int | no | `1` | |
| `oidc` | KafkaUIOidcConfig | **yes** | — | Keycloak SSO settings; the reconciler fails the CR if missing. |
| `tls` | KafkaUITlsConfig | no | `{ secretName: kafka-proxy-test-client-tls, mountPath: /etc/kafka-tls }` | Pre-provisioned client TLS Secret (cert-manager convention) mounted into the pod. |
| `discovery` | KafkaUIDiscoveryConfig | no | see below | Overrides for the env vars the UI uses to find proxy/Apicurio Services and which namespace to list `KafkaCluster` CRs from. |
| `resources` | ResourceRequirements | no | `requests: 100m/256Mi, limits: 500m/512Mi` | |
| `probes` | KafkaUIProbesConfig | no | `/q/health/ready` (5/5s) + `/q/health/live` (15/10s) | |
| `service` | KafkaUIServiceConfig | no | `NodePort 30808 -> 8080` | |
| `ingress` | KafkaUIIngressConfig | no | `{ enabled: false }` | When `enabled=true`, a single-rule Ingress is created (and removed when toggled back). |
| `env[]` | KafkaUIEnvVar | no | `[]` | Extra env vars. A name collision overrides the operator-set default. |

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
| `dnsSuffix` | `""` | Empty for in-cluster (`svc.cluster.local`). Set to `clusterset.local` for Submariner-style multi-cluster DNS. |

### status

| Field | Description |
|-------|-------------|
| `phase` | `RECONCILING` / `READY` / `FAILED` |
| `message` | Status or error message. |
| `readyReplicas` | From the underlying Deployment. |
| `observedGeneration` | Last `metadata.generation` the reconciler processed. |

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

#### Ingress-fronted (no NodePort)

```yaml
spec:
  service:
    type: ClusterIP
  ingress:
    enabled: true
    className: nginx
    host: kafka-ui.example.com
    tlsSecret: kafka-ui-tls
  oidc:
    issuerUrl: https://sso.example.com/realms/demo
    clientId: kafka-ui-web
    clientSecretRef: { name: kafka-ui-oidc, key: client-secret }
```

Toggling `ingress.enabled` back to `false` makes the reconciler delete the Ingress on the next reconcile.
