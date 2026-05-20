# Kafka client authentication: OAUTHBEARER + mTLS

This page is for Java developers writing Kafka producers or consumers against a
broker (or Kroxylicious proxy) deployed by this operator with OIDC enabled.

**TL;DR — stock Kafka 4.0 is enough.** You do *not* need a custom
`AuthenticateCallbackHandler` or an external library like Strimzi OAuth.
Everything below uses classes that ship with `org.apache.kafka:kafka-clients`.
Just configure your `Properties` and create the client.

The end-to-end working reference is
[`test-clients/schema-producer/src/main/java/se/afshin/yavari/testclients/SchemaProducerCli.java`](../test-clients/schema-producer/src/main/java/se/afshin/yavari/testclients/SchemaProducerCli.java).

---

## Property recipe

The operator's broker (and Kroxylicious proxy) listens on `SASL_SSL` with
`OAUTHBEARER` + mTLS. A minimal Java client needs three groups of properties:
mTLS keystore/truststore, SASL/OAUTHBEARER, and (optionally) Apicurio V3 serdes.

### 1. Bootstrap + TLS transport

```java
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker.example.com:9094");

props.put("security.protocol",       "SASL_SSL");
props.put("ssl.keystore.type",       "PKCS12");
props.put("ssl.keystore.location",   "/path/to/client.p12");
props.put("ssl.keystore.password",   "changeit");
props.put("ssl.truststore.type",     "PKCS12");
props.put("ssl.truststore.location", "/path/to/truststore.p12");
props.put("ssl.truststore.password", "changeit");
```

Why mTLS *and* OAUTHBEARER: TLS authenticates the client at the transport
layer (cluster membership), JWT authenticates the *user* for RBAC at the
application layer. The proxy's filter chain uses both.

### 2. SASL/OAUTHBEARER — common keys

These four keys are identical regardless of which token flow you use:

```java
props.put("sasl.mechanism", "OAUTHBEARER");
props.put("sasl.login.callback.handler.class",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
props.put("sasl.oauthbearer.token.endpoint.url", "<URL — see below>");
props.put("sasl.jaas.config",                    "<JAAS — see below>");
```

`OAuthBearerLoginCallbackHandler` is **stock Kafka**. It already supports both
flows below — the only difference is the URL scheme and the JAAS line.

---

## Two token flows

### A. Client-credentials grant (production)

The client itself POSTs `grant_type=client_credentials` to the IDP's token
endpoint at startup and on refresh. Credentials live in the JAAS line.

```java
props.put("sasl.oauthbearer.token.endpoint.url",
        "https://keycloak.example.com/realms/demo/protocol/openid-connect/token");
props.put("sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
      + "clientId=\"my-app\" "
      + "clientSecret=\"<secret>\" "
      + "scope=\"kafka\";");
```

JAAS options recognised by the stock handler: `clientId`, `clientSecret`,
`scope` (optional). Kafka caches the token and refreshes it automatically
before expiry.

### B. File-based JWT (tests, sidecar-injected tokens)

The token is written to a file by something *else* — a shell script doing
`curl ... > /tmp/token`, a sidecar, a kubelet-projected ServiceAccount token —
and Kafka reads it from disk. This is what
[`SchemaProducerCli`](../test-clients/schema-producer/src/main/java/se/afshin/yavari/testclients/SchemaProducerCli.java)
and `kind/rbac-test.sh` use.

```java
props.put("sasl.oauthbearer.token.endpoint.url", "file:///tmp/kafka-oauth-token");
props.put("sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
```

**Footgun — the `allowed.urls` JVM property.** Kafka 4.0 refuses to read any
non-HTTPS token URL unless you explicitly allow-list it. You **must** set this
**before** constructing the producer/consumer:

```java
// In code:
System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls",
        "file:///tmp/kafka-oauth-token");

// Or on the command line:
java -Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=file:///tmp/kafka-oauth-token ...
```

If you forget, the client fails at first send with an `IllegalArgumentException`
mentioning "Disallowed URL". See
[`SchemaProducerCli.java:46`](../test-clients/schema-producer/src/main/java/se/afshin/yavari/testclients/SchemaProducerCli.java)
for the pattern.

---

## Optional: Apicurio V3 JSON-schema serdes

If you want server-side schema validation through the proxy's
`RecordValidation` filter (or just want your producer to validate locally
before sending), add the Apicurio JSON-schema serializer:

```xml
<dependency>
  <groupId>io.apicurio</groupId>
  <artifactId>apicurio-registry-jsonschema-serde-kafka</artifactId>
  <version>3.0.8</version>
</dependency>
```

```java
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        JsonSchemaKafkaSerializer.class.getName());

props.put(SerdeConfig.REGISTRY_URL,               "https://apicurio-rbac-proxy.example.com:8082/apis/registry/v3");
props.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getName());
props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT,     "false");
props.put(SerdeConfig.VALIDATION_ENABLED,         "true");
```

With `TopicIdStrategy` the serializer looks up the artifact `{topic}-value`
in Apicurio, validates the payload against the schema, and emits the V3
envelope (1 magic byte `0x00` + 8-byte big-endian globalId + JSON payload).

The registry URL must go through the **apicurio-rbac-proxy** (port 8082) with
a service-account JWT — there is no direct registry Service anymore (see
`project_kroxy_filter_quirks` for history).

---

## When you actually need a custom callback handler

Strimzi OAuth and similar libraries exist for cases stock Kafka can't cover:

- **JWKS-based token introspection** (validating tokens locally against the
  IDP's public keys without re-hitting the introspection endpoint).
- **Refresh-token grant** (long-lived sessions for interactive users).
- **Kubernetes projected-SA tokens with rotation** — the kubelet rewrites the
  file periodically and stock Kafka caches the first read.

None of these apply to this operator today. If you hit one of them, that's
the point to start a real `AuthenticateCallbackHandler` implementation; until
then, the stock config above is the supported path.
