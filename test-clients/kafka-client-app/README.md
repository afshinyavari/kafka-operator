# kafka-client-app

Env-configured Quarkus producer/consumer for verifying Kafka + schema-registry setups end to
end: PLAINTEXT / mTLS / OAuth to Kafka; JSON strings or Avro; Apicurio (through the RBAC proxy
with mTLS, with OIDC, or unsecured) or Confluent Schema Registry (TLS/mTLS or unsecured).

One image, two roles. Enable the producer, the consumer, or both, with environment variables.

## Build

```bash
cd test-clients/kafka-client-app
mvn -q package
docker build -t <registry>/kafka-client-app:1.0.0 .
docker push <registry>/kafka-client-app:1.0.0
```

The module is standalone. Confluent serdes come from `https://packages.confluent.io/maven/`.

## TLS stores

Only keystore/truststore files, PKCS12 by default (JKS supported). Set the global variables
once; override per component only when something differs.

| Variable | Default |
|---|---|
| `TLS_KEYSTORE_PATH`, `TLS_KEYSTORE_PASSWORD`, `TLS_KEYSTORE_TYPE` | `PKCS12` |
| `TLS_TRUSTSTORE_PATH`, `TLS_TRUSTSTORE_PASSWORD`, `TLS_TRUSTSTORE_TYPE` | `PKCS12` |

Component prefixes with the same six suffixes: `KAFKA_TLS_`, `PRODUCER_SCHEMA_TLS_`,
`CONSUMER_SCHEMA_TLS_`. Resolution per variable: component → global → default.

## Kafka

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `PLAINTEXT`, `SSL`, `SASL_SSL` |
| `KAFKA_CLIENT_ID` | `kafka-client-app` | suffixed `-producer` / `-consumer` |
| `KAFKA_OAUTH_TOKEN_ENDPOINT`, `KAFKA_OAUTH_CLIENT_ID`, `KAFKA_OAUTH_CLIENT_SECRET` | required for `SASL_SSL` | OAUTHBEARER via Strimzi `kafka-oauth-client` |
| `KAFKA_OAUTH_SCOPE` | | optional |

`SSL` uses keystore (if set) + truststore. `SASL_SSL` uses the truststore for the broker and
for the token endpoint; the keystore is ignored.

## Producer

| Variable | Default | Notes |
|---|---|---|
| `PRODUCER_ENABLED` | `false` | |
| `PRODUCER_TOPIC` | required if enabled | |
| `PRODUCER_INTERVAL_MS` | `1000` | |
| `PRODUCER_FORMAT` | `string` | `string` (JSON) or `avro` |
| `PRODUCER_SCHEMA_REGISTRY_TYPE` | `apicurio` | `apicurio` or `confluent` |
| `PRODUCER_SCHEMA_URL` | required for `avro` | Apicurio: `https://host/apis/registry/v3`; Confluent: `https://host:8081` |
| `PRODUCER_SCHEMA_AUTH` | `none` | `none`, `mtls`, `oidc` (`oidc` not allowed with `confluent`) |
| `PRODUCER_SCHEMA_CLIENT_ID`, `PRODUCER_SCHEMA_CLIENT_SECRET`, `PRODUCER_SCHEMA_TOKEN_ENDPOINT` | required for `oidc` | |
| `PRODUCER_SCHEMA_SCOPE` | | optional |
| `PRODUCER_SCHEMA_AUTO_REGISTER` | `true` | |
| `PRODUCER_SCHEMA_GROUP` | `default` | Apicurio artifact group |

An `https` URL always applies the truststore. `mtls` additionally applies the keystore.

## Consumer

Same variables with the `CONSUMER_` prefix (`CONSUMER_SCHEMA_AUTO_REGISTER` is not used), plus:

| Variable | Default |
|---|---|
| `CONSUMER_GROUP_ID` | `kafka-client-app` |
| `CONSUMER_AUTO_OFFSET_RESET` | `earliest` |

## Payload

`Event { id: string (UUID), sequence: long, timestamp: long (epoch ms), message: string }`.
Avro schema in `src/main/avro/Event.avsc`; JSON uses the same four fields. Record key = `id`.
Registry artifact/subject is `<topic>-value` for both registry types, so through the RBAC proxy
a certificate with WRITE on the topic can register and read the schema.
Avro 1.12 only deserializes into classes it trusts; the image and `Main` set
`org.apache.avro.SERIALIZABLE_PACKAGES=se.afshin.yavari.clientapp.avro` for the generated `Event`
class.

## Deploy

```bash
kubectl apply -f deploy/tls-secrets.example.yaml         # or cert-manager-issued Secrets
kubectl apply -f deploy/configmaps/avro-apicurio-mtls-proxy.yaml   # pick a scenario
kubectl apply -f deploy/producer-deployment.yaml
kubectl apply -f deploy/consumer-deployment.yaml
kubectl logs -f deploy/kafka-client-app-producer
```

| Scenario file | Kafka | Format | Registry |
|---|---|---|---|
| `string-plaintext.yaml` | PLAINTEXT | string | – |
| `avro-apicurio-mtls-proxy.yaml` | SSL | avro | Apicurio via RBAC proxy, mTLS |
| `avro-apicurio-oidc.yaml` | SSL | avro | Apicurio, Keycloak client credentials |
| `avro-apicurio-none.yaml` | PLAINTEXT | avro | Apicurio, http, no auth |
| `avro-confluent-tls.yaml` | SSL | avro | Confluent, https + client cert |
| `avro-confluent-none.yaml` | PLAINTEXT | avro | Confluent, http, no auth |
| `kafka-oauth.yaml` | SASL_SSL | string | – |

Secrets (`*_CLIENT_SECRET`) go in a Secret referenced from `envFrom`, not in the ConfigMap.

## What the logs show

- Startup: `Invalid configuration:` + one line per problem, exit 1, if anything is missing.
- `Failed to start Kafka clients: ...` + exit 2 when a Kafka client cannot be constructed (bad
  keystore/truststore password, missing store file, invalid `CONSUMER_AUTO_OFFSET_RESET`).
- Producer: `Sent seq=<n> key=<uuid> to <topic>-<p>@<offset>` per record; `Send failed ...` WARN
  and retry on the next tick when the broker or registry rejects (e.g. 403 from the RBAC proxy).
- Consumer: `Received key=<uuid> <topic>-<p>@<offset> value=...`.
- `Consumer thread died` / `Producer thread died` at ERROR + exit 3 when a runner thread dies
  from an `Error` (e.g. `OutOfMemoryError`); the pod restarts.
- Readiness `/q/health/ready` is UP once every enabled runner has started.
