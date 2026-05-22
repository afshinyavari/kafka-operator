# Disaster recovery

What to do when something has gone wrong. Procedures are deliberately stated
as user-runnable steps — the operator does not automate DR.

## What's at risk

| Loss event | Affected data | Recovery owner |
|---|---|---|
| One MCS K8s cluster fails | Local broker + controller + proxy + Apicurio replica | KRaft quorum / MCS mesh DNS (Submariner Lighthouse, Cilium Cluster Mesh, Istio multi-cluster) — automatic if RF≥3 + min.insync.replicas=2 |
| All K8s clusters fail simultaneously | Broker on-disk topic data, Apicurio kafkasql journal, KRaft metadata | [`KafkaBackup`](api-reference.md#kafkabackup) → object storage; restore with [`KafkaRestore`](api-reference.md#kafkarestore) |
| KafkaRbac CR deleted | RBAC ConfigMaps cascade via owner-ref | Restore CR from git (GitOps source of truth) |
| KafkaCluster CR deleted | Proxy + Apicurio Deployments cascade; broker PVCs remain bound | Restore CR from git; broker PVCs re-attach by name |
| Keycloak realm corruption | OIDC tokens no longer verify | User-supplied IDP backup (see BYO IDP below) |
| Apicurio schema journal lost | Per-topic schemas | `KafkaBackup` with `includeSchemas` exports them alongside topic data; `KafkaRestore` re-imports |

## Topic data

The operator's broker pools default to RF=3 and min.insync.replicas=2 across
three MCS-joined K8s clusters. A single-cluster loss is transparent to
producers/consumers.

### Scheduled cold backup — KafkaBackup / KafkaRestore

For point-in-time cold backups beyond replication, apply a
[`KafkaBackup`](api-reference.md#kafkabackup) CR. The operator builds a `CronJob`
that runs the osodevops kafka-backup tool, copying topic **records and
consumer-group offsets** — and, with `includeSchemas`, the Apicurio schemas — to
S3 / Azure / GCS / a PVC. `spec.placement.clusterId` pins the job to one MCS
cluster so the backup runs once, not N times.

**What is _not_ covered:** topic ACLs and dynamic topic configs. Rebuild those
from your `KafkaTopic` / `KafkaRbac` CRs in git — they are the GitOps source of
truth. Keep those CRs versioned.

To restore after total loss: deploy a fresh `KafkaCluster`, then apply a
[`KafkaRestore`](api-reference.md#kafkarestore) CR (`spec.confirm: true`)
referencing the `KafkaBackup`. `restoreSchemas` imports schemas before records so
restored payloads stay decodable; `timeWindow` gives point-in-time recovery;
`topicMapping` restores into renamed (non-live) topics for inspection first.
Validate a stored backup without a full restore with
[`KafkaBackupValidation`](api-reference.md#kafkabackupvalidation).

For cross-region *live* replication, the operator also ships a [`MirrorMaker2`](api-reference.md#mirrormaker2)
CRD that drives a dedicated-mode MM2 worker Deployment. Each end (source and
target) is independently either a managed `KafkaCluster` reference (the operator
resolves to the proxy bootstrap and reuses the admin client cert) or an external
endpoint (raw bootstrap + TLS/SASL Secrets). At least one end must be managed by
this operator so it has somewhere to run.

If both ends carry Apicurio (managed) or another Apicurio-compatible registry
(external), set `spec.schemaSync.enabled=true` to enable the Apicurio-aware
schema-mirroring SMT bundled with the MM2 image. The SMT rewrites the V3 envelope
`globalId` per record to the target registry's ID, so consumers can decode
mirrored payloads. Non-Apicurio topics pass through untouched (see
`docs/api-reference.md#mirrormaker2` for the six-layer passthrough rules).

To restore from a remote cluster after total loss: deploy a fresh KafkaCluster
CR, then apply a `MirrorMaker2` CR with the source/target swapped relative to
your normal flow. `flow.replicationPolicy` (in `flow.additionalProperties` if
needed) controls topic naming.

## Apicurio schema registry

Schemas live in a Kafka topic (the `kafkasql-journal`, partition count
`1` per Apicurio v2.6's ordering requirement). Loss scenarios:

- **Topic exists, registry pods restart**: pods replay the journal at boot;
  no action needed.
- **Topic compaction collapses an unwanted state**: the journal is a
  log-compacted topic — old per-key states are gone. Restore from a remote
  Apicurio if one is configured, or re-publish from source.
- **Topic deleted entirely**: the operator-managed `KafkaTopic CR` will
  recreate the topic at the next reconcile (with `deletionPolicy=RETAIN` the
  journal data is lost). Re-publish all schemas from source.

The `KafkaBackup` `includeSchemas` flag automates this export/import. The steps
below remain a quick ad-hoc fallback:

```bash
# Dump all artifacts from the Apicurio API
TOKEN=$(curl -s -d 'client_id=...&...' ${OIDC_TOKEN_ENDPOINT} | jq -r .access_token)
curl -H "Authorization: Bearer $TOKEN" \
     http://apicurio-rbac-proxy.kafka.svc:8080/apis/registry/v3/admin/export \
     > apicurio-backup-$(date +%F).zip
```

Restore: `POST` the same zip back to `/admin/import`. Store the dumps in
your normal artifact backup channel — they're plain zip files.

## Keycloak / IDP

Out of operator scope. Use your IDP's standard backup procedure.

For the kind test rig:

```bash
# Export the demo realm
kubectl --context kind-kafka-a -n kafka exec deploy/keycloak -- \
  /opt/keycloak/bin/kc.sh export --dir /tmp/realm-export --realm demo
kubectl --context kind-kafka-a -n kafka cp \
  keycloak-XXX:/tmp/realm-export ./keycloak-realm-export
```

Restore in a fresh cluster:

```bash
kubectl --context kind-kafka-a -n kafka cp ./keycloak-realm-export \
  keycloak-XXX:/tmp/realm-import
kubectl --context kind-kafka-a -n kafka exec deploy/keycloak -- \
  /opt/keycloak/bin/kc.sh import --dir /tmp/realm-import
```

In production, the realm export should run on a schedule and ship to your
backup bucket. Test the restore quarterly.

## KRaft metadata

KRaft stores cluster metadata in a Raft log on each controller's
PersistentVolume. Loss of a single controller's PV is recoverable — KRaft
re-syncs from quorum. Loss of a quorum majority is unrecoverable; the cluster
must be re-created from scratch (topics, schemas, ACLs all rebuilt from
source).

The operator generates a deterministic 22-character cluster ID from
`namespace/name` (see `KRaftConfigGenerator.clusterIdFrom`), so re-applying
the same KafkaCluster CR produces the same cluster ID. Old PVs that survived
would re-attach correctly.

## Operator state

The operator itself is stateless. Restart strategy:

- The leader-election lease re-elects automatically (default 60s).
- In-flight reconciles drain on shutdown (`quarkus.shutdown.timeout=60S` +
  `terminationGracePeriodSeconds=90` on the Deployment, see Wave 7 #19).
- All CR status is rebuilt on the next reconcile pass.

A misbehaving operator can be quarantined by scaling its Deployment to 0
without affecting running Kafka brokers — they keep serving traffic until
the operator returns.
