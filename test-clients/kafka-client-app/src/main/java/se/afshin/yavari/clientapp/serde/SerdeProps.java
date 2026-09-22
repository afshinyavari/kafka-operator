package se.afshin.yavari.clientapp.serde;

import se.afshin.yavari.clientapp.config.SchemaRegistryConfig;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.TlsStores;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Translates a {@link SchemaRegistryConfig} into the property names the chosen registry's
 * Avro serde understands. Truststore keys are emitted whenever the URL is https; keystore
 * keys only for {@link AuthMode#MTLS}; OIDC keys only for {@link AuthMode#OIDC}.
 */
public final class SerdeProps {

    public static final String APICURIO_SERIALIZER = "io.apicurio.registry.serde.avro.AvroKafkaSerializer";
    public static final String APICURIO_DESERIALIZER = "io.apicurio.registry.serde.avro.AvroKafkaDeserializer";
    public static final String CONFLUENT_SERIALIZER = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    public static final String CONFLUENT_DESERIALIZER = "io.confluent.kafka.serializers.KafkaAvroDeserializer";

    private SerdeProps() {}

    public static Map<String, Object> producer(SchemaRegistryConfig c) {
        Map<String, Object> p = common(c);
        switch (c.type()) {
            case APICURIO -> {
                p.put("value.serializer", APICURIO_SERIALIZER);
                p.put("apicurio.registry.auto-register", c.autoRegister());
                p.put("apicurio.registry.artifact.group-id", c.group());
            }
            case CONFLUENT -> {
                p.put("value.serializer", CONFLUENT_SERIALIZER);
                p.put("auto.register.schemas", c.autoRegister());
            }
        }
        return p;
    }

    public static Map<String, Object> consumer(SchemaRegistryConfig c) {
        Map<String, Object> p = common(c);
        switch (c.type()) {
            case APICURIO -> {
                p.put("value.deserializer", APICURIO_DESERIALIZER);
                p.put("apicurio.registry.use-specific-avro-reader", true);
            }
            case CONFLUENT -> {
                p.put("value.deserializer", CONFLUENT_DESERIALIZER);
                p.put("specific.avro.reader", true);
            }
        }
        return p;
    }

    private static Map<String, Object> common(SchemaRegistryConfig c) {
        Map<String, Object> p = new LinkedHashMap<>();
        TlsStores tls = c.tls();
        switch (c.type()) {
            case APICURIO -> {
                p.put("apicurio.registry.url", c.url());
                if (c.https() && tls.hasTruststore()) {
                    p.put("apicurio.registry.tls.truststore.location", tls.truststorePath());
                    p.put("apicurio.registry.tls.truststore.password", tls.truststorePassword());
                    p.put("apicurio.registry.tls.truststore.type", tls.truststoreType());
                }
                if (c.auth() == AuthMode.MTLS) {
                    p.put("apicurio.registry.tls.keystore.location", tls.keystorePath());
                    p.put("apicurio.registry.tls.keystore.password", tls.keystorePassword());
                    p.put("apicurio.registry.tls.keystore.type", tls.keystoreType());
                }
                if (c.auth() == AuthMode.OIDC) {
                    p.put("apicurio.registry.auth.service.token.endpoint", c.oidc().tokenEndpoint());
                    p.put("apicurio.registry.auth.client.id", c.oidc().clientId());
                    p.put("apicurio.registry.auth.client.secret", c.oidc().clientSecret());
                    if (c.oidc().scope() != null) {
                        p.put("apicurio.registry.auth.client.scope", c.oidc().scope());
                    }
                }
            }
            case CONFLUENT -> {
                p.put("schema.registry.url", c.url());
                if (c.https() && tls.hasTruststore()) {
                    p.put("schema.registry.ssl.truststore.location", tls.truststorePath());
                    p.put("schema.registry.ssl.truststore.password", tls.truststorePassword());
                    p.put("schema.registry.ssl.truststore.type", tls.truststoreType());
                }
                if (c.auth() == AuthMode.MTLS) {
                    p.put("schema.registry.ssl.keystore.location", tls.keystorePath());
                    p.put("schema.registry.ssl.keystore.password", tls.keystorePassword());
                    p.put("schema.registry.ssl.keystore.type", tls.keystoreType());
                }
            }
        }
        return p;
    }
}
