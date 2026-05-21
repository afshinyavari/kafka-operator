# Disaster recovery

What to do when something has gone wrong. Procedures are deliberately stated
as user-runnable steps — the operator does not automate DR.

## What's at risk

| Loss event | Affected data | Recovery owner |
|---|---|---|
| One MCS K8s cluster fails | Local broker + controller + proxy + Apicurio replica | KRaft quorum / Submariner Lighthouse — automatic if RF≥3 + min.insync.replicas=2 |
| All K8s clusters fail simultaneously | Broker on-disk topic data, Apicurio kafkasql journal, KRaft metadata | User-supplied backups (see below) |
| KafkaRbac CR deleted | RBAC ConfigMaps cascade via owner-ref | Restore CR from git (GitOps source of truth) |
| KafkaCluster CR deleted | Proxy + Apicurio Deployments cascade; broker PVCs remain bound | Restore CR from git; broker PVCs re-attach by name |
| Keycloak realm corruption | OIDC tokens no longer verify | User-supplied IDP backup (see BYO IDP below) |
| Apicurio schema journal lost | Per-topic schemas | Re-publish from source (Apicurio export Job is on the backlog) |

## Topic data

The operator's broker pools default to RF=3 and min.insync.replicas=2 across
three MCS-joined K8s clusters. A single-cluster loss is transparent to
producers/consumers. Beyond that, the operator does not back up topic data
itself.

For cross-region replication, deploy [MirrorMaker2](https://kafka.apache.org/documentation/#georeplication-overview)
as a separate workload — the operator doesn't ship a CRD for it yet (it's on
the feature backlog). Until then, a typical setup runs MM2 as a `Deployment`
peering this cluster to a remote backup cluster, configured against the
proxy's bootstrap.

To restore from a remote cluster after total loss: deploy a fresh KafkaCluster
CR, then run MM2 in the opposite direction with `replication.policy.class`
configured for source-cluster topic naming.

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

Export procedure (manual, until the export Job ships):

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
