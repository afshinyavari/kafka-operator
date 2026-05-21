# Upgrade

Three orthogonal upgrade tracks:

1. **Operator** — the Java process that reconciles CRs.
2. **Kafka** — the broker / controller image.
3. **CRDs** — schema evolution.

## Operator upgrade

The operator runs as a `Deployment` with 1+ replicas behind a K8s leader-election
lease. Rolling the Deployment is safe:

```bash
# bump the image
kubectl -n kafka set image deploy/kafka-operator operator=kafka-operator:1.2.3
kubectl -n kafka rollout status deploy/kafka-operator
```

What happens:

1. New pod starts; old pod still holds the lease.
2. New pod becomes ready (informer caches sync), waits for the lease.
3. Old pod receives SIGTERM. `Quarkus` propagates to `JOSDK.stop()`. In-flight
   reconciles drain up to `quarkus.shutdown.timeout` (60s). Leader-election
   lease is released.
4. New pod acquires the lease and begins reconciling.

`terminationGracePeriodSeconds: 90` on the Deployment exceeds the shutdown
timeout so SIGKILL doesn't preempt the drain.

Running Kafka brokers are unaffected during an operator restart — the operator
only reconciles desired vs actual state; it isn't on the data path.

## Kafka version upgrade

The operator runs a versioned upgrade via `VersionUpgradeController`. To bump:

```yaml
# kafka-cluster.yaml
spec:
  kafkaImage: kafka-ubi:4.1.0
  kafkaVersion: "4.1"
```

The reconciler walks the upgrade phases:

1. `UPGRADING_BROKERS` — broker pool rolls one pod at a time (ISR-safe via
   AdminClient `describeTopics`).
2. `UPGRADING_CONTROLLERS` — controller pool rolls one pod at a time.
3. `WAITING_METADATA_VERSION_BUMP` — operator waits for user to bump
   `spec.config.inter.broker.protocol.version` and
   `spec.config.log.message.format.version` if applicable.
4. `IDLE` — upgrade complete.

`status.upgradePhase` and `status.currentKafkaVersion` surface the progression.
A `KafkaProxy` rolling restart kicks off automatically when its Kafka client
version changes (it follows the broker upgrade phase via the cross-cluster
roll coordinator — see `architecture.md`).

For an MCS topology, the upgrade rolls cluster A → B → C in
`spec.clusterRollOrder` order. Every cluster-spanning workload follows the
same gate: controllers, brokers, the Kroxylicious proxy Deployment, and the
Apicurio Registry Deployment. Each cluster's workloads reach READY before
the next begins, so a synchronized image bump or cert rotation can never
take down a quorum-spanning replica set simultaneously.

## CRD migration

Today the operator ships `v1alpha1` for every CRD. Breaking changes — Wave 4
(KafkaProxy + ApicurioRegistry merged into KafkaCluster, plaintext removed,
NodePort removed) — were handled as **manual migrations**, not conversion
webhooks. The procedure:

1. Stop the operator (`scale deploy/kafka-operator --replicas=0`).
2. Apply the new CRD schema (`kubectl apply -f` the generated CRDs).
3. `kubectl edit` each affected CR to migrate fields (or apply a fresh
   manifest via GitOps).
4. Start the operator. It reconciles against the new schema.

This works for `v1alpha1` because we explicitly do not promise compatibility.
Once a CRD graduates to `v1beta1` (planned for the next release after the
audit lands), the policy changes:

| API stability | Field rename | Field removal | Validation tightening |
|---|---|---|---|
| `v1alpha1` | Manual edit OK | Manual edit OK | OK |
| `v1beta1+` | Conversion webhook required | Two-version deprecation | Soft-deprecate first |

Conversion webhook scaffolding is on the backlog (Wave 7 item #18). It will
ship before any breaking change to a `v1beta1` CRD.

### Generating the new CRDs

The operator's annotation processor produces YAML under
`target/classes/META-INF/fabric8/`. Apply against each cluster:

```bash
for ctx in kind-kafka-a kind-kafka-b kind-kafka-c; do
  for crd in kafkaclusters kafkanodepools kafkapodsets kafkarbacs kafkauis kafkatopics; do
    kubectl --context "$ctx" apply -f \
      target/classes/META-INF/fabric8/${crd}.kafka.yavari.afshin.se-v1.yml
  done
done
```

The generated CRDs are also packaged with the operator image; production
GitOps should treat them as a separate artifact (Kustomize/Helm overlay)
rather than baking them into the operator manifest.

### Finalizer drain

Each CR has a finalizer that runs the reconciler's `cleanup()` on delete.
If a CR is stuck in `Terminating`, check the operator logs for the actual
failure. **Don't** strip the finalizer with `kubectl patch --type=json` as a
shortcut — that bypasses the orderly tear-down and can leave Deployments,
ConfigMaps, and Services orphaned. The audit (Wave 4b) had to do that once
when a now-deleted reconciler (the standalone `KafkaProxyReconciler`) left
finalizers on CRs whose CRD was being deleted; that's a one-shot migration
hazard, not normal practice.

## Rollback

Operator rollback: same as upgrade in reverse. `kubectl rollout undo
deploy/kafka-operator`.

Kafka version rollback: only safe **before** the cluster has been signalled to
finalize the new metadata version. Once the metadata version is bumped
(step 3 of the Kafka upgrade above), rollback requires restoring from backup.

CRD rollback: re-apply the older CRD schema and re-edit affected CRs. For
production this is also "restore from backup" — schema changes are usually
not literally reversible.
