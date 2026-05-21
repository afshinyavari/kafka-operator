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

If `clusterRollOrder` is set, controller pods wait for preceding clusters to reach `IDLE` before rolling. Broker pods roll independently without cross-cluster gating.

### Bumping the metadata version

After all pods are on the new image, advance the wire protocol:

```yaml
spec:
  targetMetadataVersion: 4   # or whatever the new version supports
```

The operator calls `AdminClient.updateFeatures()` and records the result in `status.currentMetadataVersion`.

**Downgrade protection:** setting `targetMetadataVersion` lower than `status.currentMetadataVersion` is rejected at admission. Reverting `kafkaVersion` below `status.currentKafkaVersion` is also rejected.

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
