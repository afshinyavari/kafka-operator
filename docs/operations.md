# Operations Guide

## Deploying to Kind (Development)

```bash
make mcs-setup
```

This creates 3 Kind clusters (`kafka-a`, `kafka-b`, `kafka-c`), installs Flannel CNI, deploys Submariner, loads images, installs CRDs, deploys the operator, and applies the example CRs. Total time: 3–5 minutes.

Force-rebuild Docker images if you changed the Kafka image or Dockerfile:

```bash
FORCE_BUILD=1 make mcs-setup
```

To iterate on the operator only (no cluster rebuild):

```bash
make reload-image   # ~30s: mvn build → docker build → kind load → rollout restart
```

### Verifying the setup

```bash
make quorum      # KRaft quorum health across all clusters
make status      # Pod and CR status
make pods-a      # kubectl get pods -n kafka on kafka-a
```

### End-to-end test inventory

`make -C kind e2e` runs the full integrated flow against a fresh `mcs-setup`.
Each sub-target stays runnable on its own.

| Sub-target | What it guards |
|---|---|
| `quorum` | All 3 controllers form a healthy KRaft quorum (CONTROLLER:SSL with authorizer enabled) |
| `kafkatopic-test` | `KafkaTopic` CR lifecycle: create with p=3/RF=3, alter config via AdminClient, `deletionPolicy=DELETE` vs `RETAIN` |
| `rbac-test` | Kroxylicious `GroupAwareAuthorizer`: alice→orders ALLOW, alice→invoices DENY (and bob inverse) over SASL_SSL+OAUTHBEARER |
| `apicurio-rbac-test` | Apicurio RBAC proxy enforces JWT-group-based artifact ACLs (orders-team, invoices-team, schema-admin) |
| `xml-filter-test` | Custom Kroxylicious XML-validation filter accepts schema-valid XML, rejects malformed with `INVALID_RECORD` |
| `json-schema-test` | JSON schema registered in Apicurio, fetchable, JSON messages produce/consume through KafkaProxy LB; schema rejects payloads missing required fields |
| `mcs-proxy-test` | Cross-cluster: produce via cluster-A's proxy, consume from cluster-B's AND cluster-C's via `clusterset.local` DNS |
| `proxy-external-lb-test` | Producer + consumer in a Docker container OUTSIDE Kind hit the LB-exposed proxy on MetalLB-assigned IP |
| `durability-test` | Kafkasql journal: register a schema, delete a registry pod, schema is still there after pod restart |
| `rolling-restart-test` | Annotation-kicked rolling restart of `brokers-a` NodePool: quorum stays healthy + no message loss (1 msg/sec for 60s) |
| `broker-kill-test` | Delete `brokers-a-0`; producer/consumer through cluster-B's proxy keep working (RF=3+min.isr=2); broker rejoins ISR after |
| `ui-test` | kafka-ui smoke: login via Keycloak, list clusters, browse a topic |

The KafkaProxy is exposed via MetalLB-backed LoadBalancer on every cluster
(IP pools `172.19.255.{200-210,220-230,240-250}` — reachable from the Docker
host). MetalLB install is now part of `mcs-setup.sh`; `setup-external.sh` still
exists for the alternate Gateway-API / Ingress paths.

---

## Deploying to a Real Cluster

### 1. Generate CRDs

```bash
mvn package -DskipTests -q
# CRD manifests are written to target/kubernetes/
kubectl apply -f target/kubernetes/kafkaclusters.kafka.yavari.afshin.se-v1.yml
kubectl apply -f target/kubernetes/kafkanodepools.kafka.yavari.afshin.se-v1.yml
kubectl apply -f target/kubernetes/kafkapodsets.kafka.yavari.afshin.se-v1.yml
```

### 2. RBAC

The operator needs ClusterRole permissions to manage Pods, PVCs, Services, ConfigMaps, PodDisruptionBudgets, and the custom CRDs. See `kind/manifests/rbac.yaml` for the reference ClusterRole definition.

### 3. Operator Deployment

Key environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_CLUSTER_ID` | **yes** | Must match one of `spec.clusters[].id` in the `KafkaCluster` CR for this cluster |
| `KAFKA_NETWORKING_MCS_ENABLED` | no | Set to `true` if the cluster fabric supports Submariner MCS or Cilium Cluster Mesh |

Example Deployment fragment:

```yaml
env:
  - name: KAFKA_CLUSTER_ID
    value: "A"
  - name: KAFKA_NETWORKING_MCS_ENABLED
    value: "true"
```

### 4. Leader Election

In production (`quarkus.operator-sdk.activate-leader-election-for-profiles=prod`), leader election is active. Run at least 2 replicas. The leader holds an active reconcile loop; standbys take over on failure within seconds.

---

## Kafka Version Upgrades

### Upgrading the image

1. Update `spec.kafkaImage` in `KafkaCluster` to the new image tag.
2. Optionally update `spec.kafkaVersion` (used for status tracking and downgrade protection).
3. The operator detects the config hash change, sets `upgradePhase = ROLLING`, and rolls one pod at a time using ISR/quorum safety checks.
4. When all pods are ready, `upgradePhase` returns to `IDLE`.

If `clusterRollOrder` is set, every cluster-spanning workload — controller pods, broker pods, the Kroxylicious proxy Deployment, and the Apicurio Registry Deployment — waits for preceding clusters to reach `IDLE` before rolling. A synchronized image bump or cert rotation across all three MCS clusters therefore rolls strictly one cluster at a time.

### Bumping the metadata version

After all pods are on the new image, advance the wire protocol:

```yaml
spec:
  targetMetadataVersion: 4   # or whatever the new version supports
```

The operator calls `AdminClient.updateFeatures()` and records the result in `status.currentMetadataVersion`.

**Downgrade protection:** setting `targetMetadataVersion` lower than `status.currentMetadataVersion` is rejected at admission. Reverting `kafkaVersion` below `status.currentKafkaVersion` is also rejected.

---

## Certificate Rotation

The operator does not provision TLS material — cert-manager (or whatever you use) owns the lifecycle of the Secrets it consumes. When a Secret rotates, the operator notices and rolls the affected workload so the new cert is actually in use.

### What "tracked" means

A Secret is "tracked" by a workload if the operator includes its `metadata.resourceVersion` in the workload's `configHash`. When the Secret changes, the hash flips, the PodTemplate annotation changes, Kubernetes rolls the pods. See `docs/architecture.md` → *Cert Rotation* for the implementation.

| Workload          | Tracked Secrets                                                                          |
|-------------------|------------------------------------------------------------------------------------------|
| Kafka brokers     | `{poolName}-broker-tls` (or `KafkaNodePool.spec.brokerCertSecretRef`), `{podName}-tls` per replica when `controllerTls` is set or any listener uses TLS |
| Kafka controllers | `{podName}-tls` per replica when `controllerTls` is set                                  |
| Kafka proxy       | `clientCertSecretRef` (default `{name}-client-tls`), `serverCertSecretRef` (default `{name}-server-tls`) |
| Apicurio Registry | `storage.tlsSecretRef` (kafkasql backend only)                                           |

### Rotation procedure

For cert-manager-issued Secrets the rotation is hands-off — the `Certificate` resource controls renewal cadence, cert-manager writes a new Secret, the operator's Secret informer fires, the workload rolls. For manually-rotated Secrets, just `kubectl apply` the new Secret.

Across an MCS topology, rotations triggered simultaneously on all three clusters respect `KafkaCluster.spec.clusterRollOrder` for every cluster-spanning workload: controllers, brokers, the proxy Deployment, and the Apicurio Registry Deployment. A successor cluster polls `GET /operator/upgrade-phase` on each predecessor and waits until it reports `upgradePhase=IDLE` before starting its own roll, so even synchronized cert-manager rotations roll strictly one cluster at a time.

### Verifying a rotation

```bash
# 1. Find the configHash annotation on a broker pod
kubectl --context kind-kafka-a -n kafka get pod brokers-a-0 \
  -o jsonpath='{.metadata.annotations.kafka\.yavari\.afshin\.se/config-hash}'

# 2. Trigger a rotation (cert-manager-issued; or kubectl edit secret if manual)
kubectl --context kind-kafka-a -n kafka annotate certificate brokers-a-broker-tls \
  cert-manager.io/issue-temporary-certificate=true --overwrite

# 3. Wait for the pod to roll, then re-read the annotation — it should differ
```

If the annotation does not change within ~30 seconds, check:
- The Secret's `metadata.resourceVersion` actually changed (`kubectl describe secret …`).
- The operator's Secret informer is running (`kubectl logs deploy/kafka-operator | grep -i informer`).
- The Secret name matches one of the tracked names listed above — typo'd `brokerCertSecretRef` references are silently ignored.

### Out of scope

- Signing certs / running cert-manager. The operator only consumes Secrets that already exist; it does not create CAs, `Certificate` resources, or issue requests. If `Secret` X is missing the reconciler reschedules with a message like `'X' not found in namespace ... — waiting for cert-manager / mcs-setup to create it`.
- In-place TLS reload via `incrementalAlterConfigs`. Kafka supports it; the operator chooses pod restart for consistency with all other config changes.

---

## Scaling Brokers

Change `spec.replicas` on the `KafkaNodePool`:

```bash
kubectl patch kafkanodepool brokers-a -n kafka \
  --type merge -p '{"spec":{"replicas":5}}'
```

**Scale-up:** new pods join the cluster automatically. Kafka auto-assigns partitions to new brokers if `auto.leader.rebalance.enable=true` (default).

**Scale-down:** the operator deletes one pod at a time, checking ISR safety before each deletion (no sole-ISR partitions on the departing broker). PVCs are **not deleted** — data is preserved if the replica count is later increased again.

> **Note:** The operator does not yet trigger partition reassignment before scale-down. Replicas hosted on the departing broker remain under-replicated until Kafka's own partition reassignment runs or you use Cruise Control.

---

## Kafka Proxy and RBAC

The `KafkaProxy` CRD deploys Kroxylicious as a transparent proxy in front of the broker pool.
When OIDC is configured, clients authenticate with SASL/OAUTHBEARER (JWT Bearer tokens), and
topic-level RBAC is enforced by `GroupAwareAuthorizerService` using group membership from
the JWT claims.

For Java application authors writing producers/consumers against this setup, see
[kafka-client-oauth.md](kafka-client-oauth.md) — property recipe, the two token flows,
and the `allowed.urls` JVM-property footgun.

### Prerequisites

- Keycloak (or another OIDC provider) with realm `demo`, groups `orders-team` / `invoices-team` /
  `schema-admin`, and test users `alice` (orders-team) and `bob` (invoices-team)
- Custom Kroxylicious filter image built and loaded

### Setup

```bash
# 1. Build the custom filter image (once; skip if already built)
make -C kind kroxy-image
make -C kind reload-kroxy-image

# 2. Deploy Keycloak with realm import
make -C kind keycloak-setup       # waits for Ready (up to 5 min)

# 3. Apply KafkaRbac + KafkaProxy CRs
make -C kind proxy-setup          # waits for KafkaProxy phase=READY

# 4. Run end-to-end RBAC test
make -C kind rbac-test            # 4 assertions: alice/bob × orders/invoices topics
```

### Verifying the generated proxy config

```bash
kubectl --context kind-kafka-a -n kafka \
  get cm kafka-proxy-config -o jsonpath='{.data.config\.yaml}'
```

The filter chain for OIDC mode should appear as:
`jwt-groups → oauth-bearer-validation → sasl-handshake-synthesizer → [authorization]`

### Testing from inside a broker pod (Kafka 4.0 FileTokenRetriever)

```bash
BROKER_POD=$(kubectl --context kind-kafka-a get pods -n kafka \
  -l kafka.yavari.afshin.se/node-pool=brokers-a --no-headers \
  -o custom-columns='NAME:.metadata.name' | head -1)

# Fetch JWT and write to file
kubectl --context kind-kafka-a -n kafka exec "${BROKER_POD}" -- bash -c "
  curl -sf -X POST http://keycloak.kafka.svc.cluster.local:8080/realms/demo/protocol/openid-connect/token \
    -d 'grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=alice&password=alice' \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"access_token\"], end=\"\")' \
    > /tmp/kafka-oauth-token"

# Produce via proxy (SASL/OAUTHBEARER using file:// token retriever)
kubectl --context kind-kafka-a -n kafka exec "${BROKER_POD}" -- bash -c "
  printf '%s\n' \
    'security.protocol=SASL_PLAINTEXT' \
    'sasl.mechanism=OAUTHBEARER' \
    'sasl.oauthbearer.token.endpoint.url=file:///tmp/kafka-oauth-token' \
    'sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;' \
    'sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler' \
    > /tmp/client.properties
  export KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///tmp/kafka-oauth-token'
  echo 'test-message' | timeout 20 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-proxy.kafka.svc.cluster.local:9094 \
    --topic orders \
    --producer.config /tmp/client.properties"
```

### Exposing the proxy externally

`KafkaProxy.spec.externalAccess` switches the proxy Service from ClusterIP to a publicly
reachable endpoint. Three modes are supported; each can be set independently on the same CR
and they all coexist with MCS — every targeted K8s cluster gets its own external endpoint
(the reconciler substitutes `${clusterId}` in `advertisedHostTemplate` per cluster).

Kroxylicious uses two different gateway schemes depending on mode:

| Mode | Gateway scheme | Listen ports | Client lookup |
|---|---|---|---|
| `LOADBALANCER` | `portIdentifiesNode` | `clientPort` + one per broker | Port |
| `GATEWAY` / `INGRESS` | `sniHostIdentifiesNode` | `clientPort` only | TLS SNI hostname |

#### LoadBalancer

```yaml
spec:
  externalAccess:
    type: LOADBALANCER
    # advertisedHostTemplate: ""   # optional; leave empty to auto-resolve from Service status
```

The reconciler creates the Service as `type: LoadBalancer`, watches
`Service.status.loadBalancer.ingress[0]` on each cluster, and rewrites the Kroxylicious
`advertisedBrokerAddressPattern` to the resolved LB hostname/IP. While the IP is still pending
the reconciler reschedules after 15s with a status hint.

Clients connect to `<lb-host>:<clientPort>`; the LB forwards each per-broker port to the
matching Kroxylicious listener.

#### Gateway API

```yaml
spec:
  externalAccess:
    type: GATEWAY
    advertisedHostTemplate: "${clusterId}.kafka.example.com"
    gateway:
      parentGatewayName: kafka-gateway
      parentGatewayNamespace: gateway-system
      # sectionName: tls-listener   # optional Listener name on the parent Gateway
```

The reconciler renders Kroxylicious with `sniHostIdentifiesNode` (single listen port,
SNI-based dispatch) and creates a `gateway.networking.k8s.io/v1alpha2 TLSRoute` that lists one
hostname per broker (`bootstrap.<host>`, `broker-0.<host>`, …) — all routed to the proxy
Service. The Gateway must have a TLS listener with `mode: Passthrough` so it forwards the
TLS handshake intact.

DNS: each cluster's hostname must resolve to that cluster's Gateway address — typically
`a.kafka.example.com → cluster-A gateway IP`, `b.kafka.example.com → cluster-B gateway IP`,
and so on.

#### Ingress (nginx-ingress ssl-passthrough)

```yaml
spec:
  externalAccess:
    type: INGRESS
    advertisedHostTemplate: "${clusterId}.kafka.example.com"
    ingress:
      ingressClassName: nginx
```

Same SNI-passthrough idea but using a stock `networking.k8s.io/v1 Ingress` with
`nginx.ingress.kubernetes.io/ssl-passthrough: "true"`. The ingress controller must run with
`--enable-ssl-passthrough` enabled. Each broker host gets its own rule pointing at the proxy
Service on `clientPort`.

---

## Schema Registry (Apicurio)

`ApicurioRegistry` deploys Apicurio Registry and an HTTP RBAC proxy. Clients must use the
proxy URL (`status.proxyUrl`, port 8082) with a Bearer JWT — direct registry access on port
8080 bypasses RBAC entirely.

### Prerequisites

- `make -C kind proxy-setup` already run (KafkaRbac CR and policy ConfigMap must exist)
- Keycloak already deployed (`make -C kind keycloak-setup`)

### Setup

```bash
# Build and deploy (builds image, loads into Kind, applies CR, waits for READY)
make -C kind apicurio-setup

# Run end-to-end schema registry RBAC test
# 8 assertions: alice/bob × orders/invoices × READ/WRITE
make -C kind apicurio-rbac-test
```

### Policy hot-reload

Editing `KafkaRbac` triggers the operator to update `{name}-apicurio-policy`. The proxy's
`PolicyEngine` watches the mounted file and reloads within ~1 second — no proxy restart
required.

```bash
# Verify the current policy
kubectl --context kind-kafka-a -n kafka \
  get cm kafka-rbac-apicurio-policy -o jsonpath='{.data.policy\.yaml}'
```

### Accessing the schema registry from an application

```bash
# Get a JWT
TOKEN=$(curl -sf -X POST http://<keycloak>:8080/realms/demo/protocol/openid-connect/token \
  -d 'grant_type=password&client_id=rbac-proxy&client_secret=rbac-proxy-secret&username=alice&password=alice' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"], end="")')

PROXY=http://apicurio-rbac-proxy.kafka.svc.cluster.local:8082

# Read a schema (allowed for orders-team on 'orders' artifact)
curl -H "Authorization: Bearer $TOKEN" \
  $PROXY/apis/registry/v2/groups/default/artifacts/orders

# Write a new version (allowed for orders-team on 'orders' artifact)
curl -X POST -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"type":"object"}' \
     $PROXY/apis/registry/v2/groups/default/artifacts/orders/versions

# Attempt on a forbidden artifact → 403 Forbidden
curl -H "Authorization: Bearer $TOKEN" \
  $PROXY/apis/registry/v2/groups/default/artifacts/invoices
```

### Multi-cluster HA (#19) — failover

In MCS deployments the manifest sets `spec.mcs.enabled: true` and
`spec.targetClusters: [A, B, C]`, so the same CR applied to all 3 clusters yields
3 independent Apicurio deployments sharing one kafkasql journal topic on the MCS
broker pool. Each operator creates a `ServiceExport` for its local
`{name}-rbac-proxy` Service. Clients reach the registry via the per-cluster
external URL from `status.externalUrl` (LB / Gateway / Ingress).

> **Submariner limitation**: Lighthouse does not support `LoadBalancer`-typed
> Services for cross-cluster DNS aggregation (`UnsupportedServiceType`). When
> `externalAccess.type=LOADBALANCER`, the `ServiceExport` is still created (and
> Lighthouse logs the rejection), but `apicurio-rbac-proxy.kafka.svc.clusterset.local`
> will NOT resolve. Clients must use per-cluster external URLs — typically
> through a DNS-based load balancer above MetalLB. Switching `externalAccess.type`
> to `GATEWAY` or `INGRESS` keeps the underlying Service at `ClusterIP`, in which
> case Lighthouse aggregation works.

Verify the fan-out:

```bash
# Each cluster phase should be READY
for c in a b c; do
  kubectl --context kind-kafka-$c -n kafka get apicurioregistry apicurio \
    -o jsonpath='{.status.phase}'; echo
done

# Each cluster should have its own ServiceExport for the rbac-proxy
for c in a b c; do
  kubectl --context kind-kafka-$c -n kafka get serviceexport apicurio-rbac-proxy
done

# Cross-cluster DNS: should return one endpoint per cluster (3 total)
kubectl --context kind-kafka-b -n kafka exec brokers-b-0 -- \
  nslookup apicurio-rbac-proxy.kafka.svc.clusterset.local
```

**Failover**: when `kafka-a` is unavailable, the kafkasql journal topic is still
hosted by the shared MCS broker pool (RF=3, broker pods on each cluster).
Apicurio replicas on `kafka-b` and `kafka-c` continue to read/write the journal;
clients pointed at `kafka-b`/`kafka-c`'s LB IPs continue to serve traffic.
Restart the `kafka-a` Apicurio pods once the cluster comes back — the local H2
mirror replays the journal from scratch.

**Caveats**:

- Apicurio v2.6 requires `storage.kafkaTopicPartitions: 1` for total ordering;
  the operator warns if overridden. With multi-cluster HA this becomes
  load-bearing — keep it at 1.
- Concurrent writes to the same artifact-version from different cluster's
  clients are resolved by Apicurio's optimistic concurrency (one client gets
  `409 Conflict`). Safe but visible to clients.
- All 3 deployments must use the same `storage.kafkaTopic` and
  `storage.principal` (automatic when applying the same CR).
- For cross-cluster failover via DNS, put a DNS-based load balancer (or
  HAProxy / cloud-LB) in front of the 3 per-cluster LB IPs. Submariner
  Lighthouse cannot do this for LoadBalancer-typed Services.

---

## Kafka UI (Web Console)

Apply via:

```bash
make -C kind kafka-ui-setup
```

This now deploys the UI to **all 3 clusters** (HA #20). Per-cluster external
URLs are available at `status.advertisedHost`. State is read-mostly (per-pod
Caffeine cache + per-pod OIDC session), so HA is active-active with eventual
freshness — no shared session store required.

The kafka-ui Service is typically `LoadBalancer` (matching the existing
`externalAccess.type: LOADBALANCER` default), which Submariner Lighthouse
rejects for cross-cluster DNS. Browsers reach each cluster via the per-cluster
LB IP; for unified DNS, layer a DNS-based load balancer above MetalLB.

```bash
# Verify the per-cluster external URL
for c in a b c; do
  kubectl --context kind-kafka-$c -n kafka get kafkaui kafka-ui \
    -o jsonpath='{.status.phase}{"  "}{.status.advertisedHost}'; echo
done
```

If browser clients are pinned to one cluster's LB IP, OIDC sessions remain on
that cluster — sticky by IP rather than by user. For cross-cluster failover
(load balancer drops a backend), point users at the cluster-local URL or use a
DNS record that resolves to all 3 LB IPs.

### Phase 2 write operations

The UI is no longer read-only. Authenticated users can issue writes whose
permissions are enforced downstream by Kafka (via Kroxylicious) or Apicurio:

| Operation | UI surface | Underlying call | Enforcer |
|---|---|---|---|
| Create topic | Topics list → "New topic" modal | `AdminClient.createTopics` | Broker (Kroxy RBAC) |
| Alter topic configs | Topic detail → "Edit configs" modal | `AdminClient.incrementalAlterConfigs` | Broker |
| Delete topic | Topic detail → "Delete" modal (typed confirm) | `AdminClient.deleteTopics` | Broker |
| Produce message | Message browser → "Produce" modal | `KafkaProducer.send` (acks=all, 5s timeout) | Broker |
| Reset consumer offsets | Groups → ⋮ → "Reset offsets" modal | `AdminClient.alterConsumerGroupOffsets` (EARLIEST / LATEST / explicit) | Broker |
| Delete consumer group | Groups → ⋮ → "Delete" modal (typed confirm) | `AdminClient.deleteConsumerGroups` | Broker |
| Create schema | Schemas → "New schema" modal | `POST /apis/registry/v2/groups/default/artifacts` | apicurio-rbac-proxy |
| New schema version | Schema detail → "New version" modal | `PUT /apis/registry/v2/groups/default/artifacts/{id}` | apicurio-rbac-proxy |
| Delete schema | Schema detail → "Delete" modal (typed confirm) | `DELETE /apis/registry/v2/groups/default/artifacts/{id}` | apicurio-rbac-proxy |

ACL editing is **not** exposed in the UI; `KafkaRbac` CRs continue to be
managed via GitOps. See [security.md → UI Phase 2 trust model](security.md#ui-phase-2-trust-model).

### Audit log

Every write emits one JSON line on the `kafka-ui.audit` logger:

```bash
# Tail audit events from any single pod
kubectl --context kind-kafka-a -n kafka logs -l app=kafka-ui --tail=200 \
  | grep '"action"' | jq -c 'select(.action)'
```

Fields: `ts`, `user`, `action` (e.g. `topic.create`, `message.produce`,
`group.resetOffsets`, `schema.delete`), `target` (`cluster/object`), `outcome`
(`success` / `failure`), optional `details`, and `correlationId` (from the
operator-wide MDC).

### CSRF

State-changing requests (POST/PUT/PATCH/DELETE) are checked by
`OriginCsrfFilter`: the `Origin` header (or `Referer` fallback) must match
the `Host` header, or appear in `kafka-ui.csrf.allowed-origins` (comma-separated).
SameSite=Lax on the OIDC session cookie is the primary protection; this filter
is defence-in-depth. Disable with `kafka-ui.csrf.enabled=false` only for
explicit embed scenarios.

### Troubleshooting writes

| Symptom | Likely cause |
|---|---|
| Write button returns "Forbidden" or HTTP 403 | User's KafkaRbac group lacks the relevant operation (`CREATE`, `DELETE`, `WRITE`, `ALTER`, `DESCRIBE_CONFIGS`). Update the `KafkaRbac` CR. |
| "CSRF: origin mismatch" 403 | Browser sent an `Origin` that doesn't match the UI's host. Verify the user is on the canonical URL, not a stale proxy. |
| "Schema registry denied access (HTTP 401/403)" | apicurio-rbac-proxy rejected the user's JWT — check the `KafkaRbac.spec.groups[].schemaRegistry.actions` list. |
| Topic created via UI but not visible in `kubectl get kafkatopic` | Intentional: Phase 2 UI writes go directly to Kafka via AdminClient, no `KafkaTopic` CR is created. To track via CR, create the `KafkaTopic` resource separately. |

---

## Monitoring

### Prometheus metrics

The operator exposes metrics at `http://{operator-pod}:8080/metrics` in Prometheus format.

| Metric | Labels | Description |
|--------|--------|-------------|
| `kafka_operator_rolling_updates_total` | `namespace`, `pool`, `result` (success/failure) | Rolling update attempt count |
| `kafka_operator_rolling_update_duration_seconds` | `namespace`, `pool` | Time to complete a pod restart |
| `kafka_operator_isr_checks_total` | `type` (broker/controller), `result` (safe/unsafe) | ISR/quorum safety check outcomes |
| `kafka_operator_scale_down_attempts_total` | `namespace`, `pool`, `result` (proceeded/blocked) | Scale-down attempts; blocked = ISR not yet safe |

JVM metrics (heap, GC, threads) are also exposed via Micrometer.

### Prometheus ServiceMonitor (optional)

Enable automatic scraping via the Prometheus Operator:

```yaml
spec:
  metricsConfig:
    configMapRef: my-jmx-config   # ConfigMap with jmx-config.yaml key
```

This creates a `ServiceMonitor` and a `Service` (port 9101) per node pool for Kafka JMX metrics, in addition to the operator's own `/metrics` endpoint.

### Health endpoint

```
GET http://{operator-pod}:8080/healthz
```

Returns HTTP 200 when the operator is healthy (Quarkus SmallRye Health format).

---

## Troubleshooting

### Pod stuck in CrashLoopBackOff

```bash
kubectl logs {pod} -n kafka
```

Common causes:

| Symptom in logs | Cause | Fix |
|-----------------|-------|-----|
| `Unable to parse INTERNAL://null` | `brokerAdvertisedAddress` not resolved | Check `POD_NAME` env var is injected; verify ConfigMap template |
| `Error creating broker listeners` | Listener port conflict | Check that no two listeners share the same port (9092 and 9093 are reserved) |
| TLS exception at startup | Missing or malformed TLS secret | Verify Secret `{podName}-tls` exists with `tls.crt`, `tls.key`, `ca.crt` |
| `kafka-storage.sh format` fails | Corrupt or incompatible data directory | Check PVC; may need to delete PVC and let operator recreate |

### Quorum unhealthy

```bash
make quorum
```

If a cluster shows `✗ Quorum check failed`:

1. Check Submariner gateway pods: `kubectl --context kind-kafka-{a,b,c} -n submariner-operator get pods`
2. Verify cross-cluster DNS: `kubectl exec -n kafka {broker-pod} -- nslookup controllers-a-headless.kafka.svc.clusterset.local`
3. Check controller pod logs for election errors

### Rolling update stuck

Check `KafkaPodSet` status:

```bash
kubectl get kafkapodset {pool}-podset -n kafka -o jsonpath='{.status.currentRollingPod}'
```

If non-empty and not progressing:
- **ISR blocked:** One partition may have only one in-sync replica. Wait for replication to catch up, or check if a broker is down.
- **Cross-cluster gate:** Check that the preceding cluster in `clusterRollOrder` has `upgradePhase=IDLE`:
  ```bash
  kubectl get kafkacluster my-kafka -n kafka -o jsonpath='{.status.upgradePhase}'
  ```
- **Pod not becoming Ready:** Check pod logs and readiness probe; default probe is TCP on port 9092 (or first TLS listener port).

### Proxy and RBAC issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| All RBAC denied; proxy logs `subject=Subject[principals=[]]` | Filter chain order wrong; `OauthBearerValidationFilter` processes the broker's SASL error before `SaslHandshakeSynthesizerFilter` converts it to success, so `clientSaslAuthenticationSuccess()` is never called | Ensure chain order: `jwt-groups → oauth-bearer-validation → sasl-handshake-synthesizer` |
| `TopicAuthorizationException` for METADATA requests despite valid JWT | `DESCRIBE` operation not covered by RBAC rules | Use semantic operations `PRODUCE`/`FETCH` which alias to `{WRITE, DESCRIBE}` / `{READ, DESCRIBE}` |
| Proxy pod logs `OIDC discovery failed` or `Connection refused` to Keycloak | Wrong realm in `spec.oidc.jwksEndpointUrl` or Keycloak not yet ready | Check the realm name matches the realm imported into Keycloak; run `make keycloak-setup` if Keycloak is missing |
| ConfigMap `kafka-proxy-config` not updated after `KafkaRbac` change | Field manager conflict from prior manual `kubectl apply` | Delete and recreate the ConfigMap, or let the operator manage it exclusively via `createOrReplace` |
| Schema registry proxy returns 502 Bad Gateway | `apicurio-registry` service not reachable from proxy pod | Verify `apicurio-registry` Service exists and registry pod is Running |
| Schema registry proxy throws `IllegalArgumentException: :status` | HTTP/2 pseudo-headers forwarded into HTTP/1 response | Fixed in `ProxyResource.forward()` — skip headers starting with `:`. Rebuild proxy image if on an old version. |
| Schema registry: 403 Forbidden for an expected-allowed call | RBAC policy not yet reloaded after `KafkaRbac` update | Wait ~1–2 s for `PolicyEngine` watcher to hot-reload; check proxy logs for `[PolicyEngine] Loaded N rules` |

### Checking generated server.properties

```bash
kubectl exec {broker-pod} -n kafka -- cat /tmp/server.properties
```

And the startup script:

```bash
kubectl exec {broker-pod} -n kafka -- cat /opt/kafka-config/start.sh
```

### Operator logs

```bash
make logs-a   # tail kafka-a operator
# or directly:
kubectl logs -n kafka -l app=kafka-operator --context kind-kafka-a -f
```

Log level for `se.afshin.yavari` is `DEBUG` by default; JOSDK and Fabric8 are `INFO`/`WARN` to reduce noise.

## Prerequisites for production

The operator deliberately does *not* manage two pieces of infrastructure. Run
them yourselves before deploying the operator into a production namespace.

### Bring your own IDP

The operator consumes OIDC config (`spec.proxy.oidc.*`) but does not deploy
Keycloak or any other IDP. In production:

- The IDP must be reachable from the proxy pod at the configured
  `jwksEndpointUrl`. Use HTTPS — JWT signature verification over HTTP is a
  trivial MITM target.
- The `expectedIssuer` must match the IDP's `iss` claim exactly (scheme +
  authority + path, no trailing slash unless your IDP emits one).
- The `expectedAudience` must match the JWT `aud` claim. Configure your
  client application's audience at the IDP to match.
- `groupsClaim` defaults to `realm_access.roles` (the Keycloak shape). For
  other IDPs map it to whatever claim holds the user's group/role membership.

The `kind/manifests/keycloak*.yaml` fixtures are test-rig only — they exist
so `make e2e` is self-contained. They use HTTP and a static realm definition.
**Do not** copy them into a production environment.

See `docs/security.md` and `docs/kafka-client-oauth.md` for client-side
configuration.

### Bring your own cert-manager

The operator does *not* sign or rotate certificates. It expects Secrets to
already exist by name, with the standard `tls.crt` / `tls.key` / `ca.crt`
keys (the cert-manager convention).

Cert sources the operator reads:

| Secret name (default) | Mounted by | Field that overrides |
|---|---|---|
| `kafka-operator-client-tls` | Operator's AdminClient | `spec.proxyMtls.adminClientCertSecretRef` |
| `kafka-proxy-client-tls` | Proxy → broker | `spec.proxy.tls.clientCertSecretRef` |
| `kafka-proxy-server-tls` | Client → proxy | `spec.proxy.tls.serverCertSecretRef` |
| `schema-registry-client-tls` | Apicurio → broker (kafkasql) | `spec.apicurio.storage.tlsSecretRef` |
| `{poolName}-broker-tls` | Broker INTERNAL listener | `KafkaNodePool.spec.brokerCertSecretRef` |

Rotation policy is yours. The operator reacts to in-place Secret rotation
automatically (see *Cert rotation* in `docs/security.md`) — within seconds of
cert-manager updating a Secret, the affected pod's `configHash` changes and
the Pod is rolled.

In the kind rig, `kind/mcs-setup.sh` provisions self-signed certs into the
same Secret names so the rest of the operator works unchanged.

### Per-user Kafka quotas

Wave 7 (#21) added `spec.users[].quotas` to `KafkaRbac`. Apply broker-side
client quotas via the same CR that defines the user:

```yaml
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaRbac
metadata:
  name: kafka-rbac
spec:
  users:
    - name: alice
      kafka:
        topics: [orders]
        operations: [PRODUCE]
      quotas:
        producerByteRate: 1048576       # 1 MiB/s
        consumerByteRate: 2097152       # 2 MiB/s
        requestPercentage: 0.5          # 50% of one IO thread
        controllerMutationRate: 10.0    # 10 admin ops/sec
```

All quota fields are optional; unset fields are not pushed to the broker (so
they don't disturb manually-applied quotas on the same user). The operator
applies quotas via AdminClient on the primary cluster only — under MCS, only
the first entry in `KafkaCluster.spec.clusters[]` issues the
`alterClientQuotas` call.
