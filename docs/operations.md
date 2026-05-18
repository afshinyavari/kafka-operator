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
