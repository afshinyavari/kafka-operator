# Running the schema-sync SMT in Strimzi

Two files:

- `Dockerfile` — bakes the shaded SMT JAR into a Strimzi Kafka image under
  `/opt/kafka/plugins/schema-sync-smt/`.
- `kafka-mirrormaker2-example.yaml` — a `KafkaMirrorMaker2` CR: Confluent Schema
  Registry source (plain HTTP, no auth) → Strimzi target Kafka over mTLS, with
  Apicurio Registry (native API) as the target registry over mTLS.

## Build the image

```bash
cd schema-sync-smt
mvn -q package -DskipTests
docker build -f strimzi/Dockerfile \
  --build-arg STRIMZI_IMAGE=quay.io/strimzi/kafka:0.47.0-kafka-4.0.0 \
  -t registry.example.com/strimzi-kafka-schema-sync:0.47.0-kafka-4.0.0 .
docker push registry.example.com/strimzi-kafka-schema-sync:0.47.0-kafka-4.0.0
```

Use the same Strimzi/Kafka tag your operator runs. The JAR is compiled for Java 17.

## Certificates for the target registry

Apicurio needs a client certificate it trusts. Put it in a Secret with PEM files;
the key must be PKCS#8 (`-----BEGIN PRIVATE KEY-----`):

```bash
openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key   # only if the key is PKCS#1/SEC1
kubectl -n kafka create secret generic mm2-registry-target-tls \
  --from-file=tls.crt=client.crt \
  --from-file=tls.key=client-pkcs8.key \
  --from-file=ca.crt=registry-ca.crt
```

PKCS12/JKS work too: set `ssl.keystore.type: PKCS12`, point `ssl.keystore.location`
at the `.p12` and supply `ssl.keystore.password` (Strimzi's
`KubernetesSecretConfigProvider` can inject it as `${secrets:ns/name:key}`).

## Apply

```bash
kubectl apply -f strimzi/kafka-mirrormaker2-example.yaml
kubectl -n kafka logs deploy/prod-to-dr-mirrormaker2 -f | grep -i schemaSync
```

The worker logs `ApicurioSchemaTransferSmt configured: source=… (CONFLUENT), target=… (APICURIO)`
on start. Records on the mirrored topics arrive on the target with the 9-byte
Apicurio envelope; their schemas appear in Apicurio as `prod.<topic>-value`.

## Notes

- `target.subject.prefix` should equal the source alias plus a dot when using
  Strimzi's default replication policy. Drop it with `IdentityReplicationPolicy`.
- Every `auth.*` / `ssl.*` key is optional per side; only `url` and `format` are needed.
- The full key reference is in `docs/api-reference.md` ("SMT configuration keys");
  behaviour on non-schema topics in `docs/operations.md`.
