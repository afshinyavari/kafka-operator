# kafka-client-app — configurable Quarkus producer/consumer test client

**Date:** 2026-09-22
**Status:** approved design, pending implementation plan
**Location:** `test-clients/kafka-client-app` (standalone module, not part of the operator build)

## Purpose

A small Quarkus application that produces and/or consumes Kafka records, configured
entirely through environment variables, so it can be deployed on OpenShift (or any
Kubernetes) to verify end to end:

- Kafka connectivity with PLAINTEXT, mTLS or SASL/OAUTHBEARER.
- Schema-registry access through the Apicurio RBAC proxy (mTLS, authorized by Kafka ACLs),
  directly against an OIDC-secured Apicurio (Keycloak), against an unsecured Apicurio, and
  against a Confluent Schema Registry (TLS/mTLS or none).
- Both wire formats: plain JSON strings and Avro through the registry serde.

It replaces nothing. `test-clients/schema-producer` stays as is.

## Non-goals

- PEM certificates. Only keystore/truststore files (PKCS12 default, JKS supported). The
  target environment mounts cert-manager-issued PKCS12 stores.
- Apicurio Registry 2.x API. Only the v3 API. (The 3.3.x serde can be pointed at a v2 API
  with `apicurio.registry.url.version=v2`, but this is not exposed or tested.)
- Basic auth to any registry, OIDC to Confluent Schema Registry.
- Metrics, Kafka Streams, transactions, custom schemas from files.
- Integration into this repository's `mcs-setup` / e2e suites (explicitly skipped by the
  user on 2026-09-22). Verification happens in the user's OpenShift environment.

## Versions

| Component | Version | Source |
|---|---|---|
| Quarkus | 3.39.4 (pins `kafka-clients` 4.2.1, Vert.x 4.5) | Maven Central |
| Apicurio serde | `io.apicurio:apicurio-registry-avro-serde-kafka` 3.3.3 | Maven Central |
| Confluent serde | `io.confluent:kafka-avro-serializer` 8.3.2 | `https://packages.confluent.io/maven/` (`<repository>` in module pom) |
| Strimzi OAuth | `io.strimzi:kafka-oauth-client` 0.18.0 | Maven Central |
| Avro | as pulled by Quarkus BOM / `avro-maven-plugin` matching | |
| Java | 21, UBI 9 `openjdk-21-runtime` | registry.access.redhat.com |

`kafka-clients` is managed to the Quarkus BOM version; Confluent's `*-ccs` Kafka artifacts
are excluded from the Confluent dependency.

## Environment variables

All configuration is environment variables. Names are exact; values are case-insensitive
where enumerated.

### Global TLS fallback

| Variable | Default | Notes |
|---|---|---|
| `TLS_KEYSTORE_PATH` | | Client certificate + key |
| `TLS_KEYSTORE_PASSWORD` | | |
| `TLS_KEYSTORE_TYPE` | `PKCS12` | `PKCS12` or `JKS` |
| `TLS_TRUSTSTORE_PATH` | | CA bundle |
| `TLS_TRUSTSTORE_PASSWORD` | | |
| `TLS_TRUSTSTORE_TYPE` | `PKCS12` | `PKCS12` or `JKS` |

Each TLS-using component has its own prefix with the same six suffixes:
`KAFKA_TLS_*`, `PRODUCER_SCHEMA_TLS_*`, `CONSUMER_SCHEMA_TLS_*`. Resolution is per
variable: a component variable, if set, wins; otherwise the global one; otherwise the
default. So one can override only the truststore for the registry and keep the global
keystore.

### Kafka

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `PLAINTEXT`, `SSL`, `SASL_SSL` |
| `KAFKA_TLS_*` | global | `SSL`: keystore (optional — omit for truststore-only listeners) + truststore → `ssl.keystore.*`, `ssl.truststore.*`. `SASL_SSL`: truststore only (keystore ignored) |
| `KAFKA_OAUTH_TOKEN_ENDPOINT` | required for `SASL_SSL` | Keycloak token endpoint |
| `KAFKA_OAUTH_CLIENT_ID` | required for `SASL_SSL` | |
| `KAFKA_OAUTH_CLIENT_SECRET` | required for `SASL_SSL` | |
| `KAFKA_OAUTH_SCOPE` | | optional |
| `KAFKA_CLIENT_ID` | `kafka-client-app` | `client.id`; producer/consumer append `-producer`/`-consumer` |

`SASL_SSL` sets `sasl.mechanism=OAUTHBEARER`,
`sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler`
and the `oauth.*` JAAS options; the resolved truststore is also passed as
`oauth.ssl.truststore.*` so the token endpoint's CA is trusted.

### Producer

| Variable | Default | Notes |
|---|---|---|
| `PRODUCER_ENABLED` | `false` | |
| `PRODUCER_TOPIC` | required if enabled | |
| `PRODUCER_INTERVAL_MS` | `1000` | one record per interval |
| `PRODUCER_FORMAT` | `string` | `string` (JSON as `StringSerializer`) or `avro` |
| `PRODUCER_SCHEMA_REGISTRY_TYPE` | `apicurio` | `apicurio` or `confluent`; only read when format is `avro` |
| `PRODUCER_SCHEMA_URL` | required for `avro` | Apicurio: `https://host:8443/apis/registry/v3`. Confluent: `https://host:8081` |
| `PRODUCER_SCHEMA_AUTH` | `none` | `none`, `mtls`, `oidc`. `oidc` rejected for `confluent` |
| `PRODUCER_SCHEMA_TLS_*` | global | Truststore applied whenever the URL is `https`. Keystore applied only for `mtls` |
| `PRODUCER_SCHEMA_CLIENT_ID` | required for `oidc` | |
| `PRODUCER_SCHEMA_CLIENT_SECRET` | required for `oidc` | |
| `PRODUCER_SCHEMA_TOKEN_ENDPOINT` | required for `oidc` | |
| `PRODUCER_SCHEMA_SCOPE` | | optional |
| `PRODUCER_SCHEMA_AUTO_REGISTER` | `true` | |
| `PRODUCER_SCHEMA_GROUP` | `default` | Apicurio artifact group; ignored for Confluent |

### Consumer

Mirrors the producer with `CONSUMER_` prefix: `CONSUMER_ENABLED`, `CONSUMER_TOPIC`,
`CONSUMER_FORMAT`, `CONSUMER_SCHEMA_REGISTRY_TYPE`, `CONSUMER_SCHEMA_URL`,
`CONSUMER_SCHEMA_AUTH`, `CONSUMER_SCHEMA_TLS_*`, `CONSUMER_SCHEMA_CLIENT_ID`,
`CONSUMER_SCHEMA_CLIENT_SECRET`, `CONSUMER_SCHEMA_TOKEN_ENDPOINT`, `CONSUMER_SCHEMA_SCOPE`,
`CONSUMER_SCHEMA_GROUP`, plus:

| Variable | Default |
|---|---|
| `CONSUMER_GROUP_ID` | `kafka-client-app` |
| `CONSUMER_AUTO_OFFSET_RESET` | `earliest` |

`CONSUMER_SCHEMA_AUTO_REGISTER` is not read (deserializers never register). Producer and
consumer may both be enabled in one pod; at least one must be enabled.

### Serde property mapping

| Concern | Apicurio (`apicurio.registry.*`) | Confluent (`schema.registry.*` / top-level) |
|---|---|---|
| URL | `url` | `schema.registry.url` |
| Truststore | `tls.truststore.location/password/type` | `schema.registry.ssl.truststore.location/password/type` |
| Keystore (mtls) | `tls.keystore.location/password/type` | `schema.registry.ssl.keystore.location/password/type` |
| OIDC | `auth.service.token.endpoint`, `auth.client.id`, `auth.client.secret`, `auth.client.scope` | n/a (rejected) |
| Auto-register | `auto-register` | `auto.register.schemas` |
| Naming | `artifact-resolver-strategy` = `TopicIdStrategy` (default), `artifact.group-id` | `value.subject.name.strategy` = `TopicNameStrategy` (default) |
| Serializer class | `io.apicurio.registry.serde.avro.AvroKafkaSerializer` / `AvroKafkaDeserializer` | `io.confluent.kafka.serializers.KafkaAvroSerializer` / `KafkaAvroDeserializer` |
| Specific record | `apicurio.registry.use-specific-avro-reader=true` (consumer) | `specific.avro.reader=true` (consumer) |

Both registries therefore name the schema `<topic>-value`, matching the RBAC proxy's
artifact-to-topic mapping.

## Components

Package `se.afshin.yavari.clientapp`. All env access goes through a
`Function<String,String>` so every class is unit-testable without setting real
environment variables.

```
config/
  Env                  thin wrapper: get(name), get(name, default), require(name), enum parsing
  TlsStores            record(keystorePath/password/type, truststorePath/password/type);
                       static resolve(env, prefix) applies component→global→default fallback
  KafkaClientConfig    env → Properties for kafka-clients (bootstrap, security protocol, ssl.*, sasl/oauth)
  SchemaRegistryConfig env(prefix) → validated record (type, url, auth, tls, oidc creds, autoRegister, group)
  ProducerConfig       enabled, topic, intervalMs, format, schema (SchemaRegistryConfig or null)
  ConsumerConfig       enabled, topic, groupId, autoOffsetReset, format, schema
  AppConfig            aggregates all of the above; validate() collects all errors and fails once
serde/
  SerdeProps           SchemaRegistryConfig → Map<String,Object> serde props + serializer/deserializer class names
producer/
  Event                Avro SpecificRecord generated from src/main/avro/Event.avsc
  PayloadGenerator     next() → Event (id, sequence, timestamp, message); asJson(Event) → String
  ProducerRunner       plain class; scheduled thread sends one record per interval; onThreadDeath
                       callback fires (after a guarded producer close) if a tick throws an Error
consumer/
  ConsumerRunner       plain class; dedicated thread polling and logging; onThreadDeath callback
                       fires (after a guarded consumer close) if the poll thread dies from an Error
runtime/
  ClientFactory        KafkaClientConfig + ProducerConfig/ConsumerConfig → final kafka-clients Properties
  Runners              start(AppConfig, RunnerRegistry[, onThreadDeath]) builds and starts the
                       enabled runners, rolling back (stopping) anything already started if a
                       later step throws; stopAll() stops consumer then producer, never throws.
                       The default onThreadDeath calls Quarkus.asyncExit(EXIT_THREAD_DEATH=3)
  RunnerRegistry       @ApplicationScoped; started-state of every enabled runner for the readiness probe
  RunnersReadyCheck    @Readiness: UP when every enabled runner reports started
Main                  @QuarkusMain: loads AppConfig, validates, exits 1 with message on failure
```

Event schema (`Event.avsc`, namespace `se.afshin.yavari.clientapp.avro`):

| Field | Type | Value |
|---|---|---|
| `id` | string | UUID |
| `sequence` | long | 0, 1, 2, … from process start |
| `timestamp` | long | epoch millis |
| `message` | string | `"hello from <hostname> #<sequence>"` |

JSON string format uses the same four field names, serialized with Jackson. Record key is
always `id` as string with `StringSerializer`.

## Runtime behaviour

**Startup.** `Main` builds `AppConfig` from `System.getenv`, validates, and on any error
prints every problem (one per line, naming the variable) to stderr and exits with code 1.
Runners are then created from the validated config. Producer and consumer clients are built
with the `Properties` from `KafkaClientConfig` plus `SerdeProps`, passing serializer classes
by name (no programmatic construction).

**Producer.** A single-thread scheduled executor fires every `PRODUCER_INTERVAL_MS`. Each
tick generates one `Event`, serializes according to format, and calls `producer.send` with a
callback. Success logs `topic-partition@offset` at INFO. Failure (callback exception or a
synchronous `SerializationException` from the registry) logs at WARN with the cause and the
tick ends; the next tick retries. Producer is flushed and closed on shutdown.

**Consumer.** A dedicated daemon thread subscribes to `CONSUMER_TOPIC` and polls with a
1-second timeout. Every record is logged at INFO with key, partition, offset and value
(`Event.toString()` for Avro, raw string otherwise). Exceptions inside the loop are logged
at WARN and the loop continues (a `WakeupException` on shutdown ends it). Auto-commit is
left on. The consumer is closed on shutdown.

**Health.** `/q/health/ready` reports UP when all enabled runners have started their
thread/executor; `/q/health/live` is always UP while the process runs.

**Thread death.** If a runner's dedicated thread dies from an escaped `Error` (e.g. an
`OutOfMemoryError`), the runner closes its Kafka client (guarded) and the process exits with
code 3, so the pod restarts instead of running on with a dead producer or consumer.

## Deployment artifacts

- `Dockerfile`: UBI 9 `openjdk-21-runtime`, copies `target/quarkus-app`, exposes 8080.
- `deploy/producer-deployment.yaml` and `deploy/consumer-deployment.yaml`: one container,
  Secret volumes for `keystore.p12` and `truststore.p12` mounted at `/etc/tls/`, `envFrom`
  a ConfigMap and a Secret for passwords, readiness probe on `/q/health/ready`.
- `deploy/configmaps/`: one ConfigMap per scenario, each fully commented:
  `string-plaintext.yaml`, `avro-apicurio-mtls-proxy.yaml`, `avro-apicurio-oidc.yaml`,
  `avro-apicurio-none.yaml`, `avro-confluent-tls.yaml`, `avro-confluent-none.yaml`,
  and `kafka-oauth.yaml` showing `SASL_SSL`.
- `README.md`: build, image, full variable table (copied from this spec), scenario matrix,
  and what to look for in the logs.

## Testing

Unit tests (JUnit 5, no Quarkus test runtime, no Kafka):

- `TlsStoresTest`: global only; component override of a single variable; type defaults.
- `KafkaClientConfigTest`: PLAINTEXT emits no ssl keys; SSL emits keystore + truststore;
  SASL_SSL emits mechanism, callback handler, `oauth.*` and truststore but no keystore;
  missing bootstrap / missing oauth vars are reported.
- `SchemaRegistryConfigTest`: prefix handling; `oidc`+`confluent` rejected; `avro` without
  URL rejected; `mtls` without keystore rejected; `https` URL with `none` still resolves the
  truststore; `http` URL emits no TLS keys.
- `SerdePropsTest`: exact property names for both registry types for each auth mode;
  serializer/deserializer class names; consumer specific-reader flag.
- `PayloadGeneratorTest`: sequence increments; JSON has exactly the four fields; Avro record
  round-trips through its own schema.
- `AppConfigTest`: neither runner enabled is an error; all errors are collected and reported
  together.

Build verification: `mvn package` in the module and `docker build` succeed. Runtime
verification is manual in the user's OpenShift environment; this repository's e2e suites
are not touched.
