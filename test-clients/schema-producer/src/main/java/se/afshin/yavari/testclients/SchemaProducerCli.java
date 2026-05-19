package se.afshin.yavari.testclients;

import io.apicurio.registry.serde.config.SerdeConfig;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * End-to-end producer for the schema-registry-validation test. Two modes:
 *
 *   --mode valid    Uses {@link JsonSchemaKafkaSerializer} with
 *                   {@link TopicIdStrategy} so Apicurio fetches the latest
 *                   schema for "{topic}-value", validates the payload locally,
 *                   and emits a V3-enveloped record. The Kroxylicious
 *                   RecordValidation filter at the proxy must accept it.
 *
 *   --mode invalid  Skips the Apicurio serializer and emits raw bytes:
 *                   the V3 envelope (magic byte 0x00 + 8-byte globalId) wrapped
 *                   around a schema-violating JSON payload. The proxy must
 *                   reject with INVALID_RECORD.
 *
 * Auth: SASL_SSL (mTLS keystore + OAUTHBEARER, JWT from a file).
 */
public class SchemaProducerCli {

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        String mode = required(a, "mode");

        // Kafka client requires this JVM property to honour file:// token URLs.
        String tokenFile = required(a, "token-file");
        System.setProperty("org.apache.kafka.sasl.oauthbearer.allowed.urls", "file://" + tokenFile);

        Properties props = baseProducerProps(a);

        if ("valid".equals(mode)) {
            produceValid(props, a);
        } else if ("invalid".equals(mode)) {
            produceInvalid(props, a);
        } else {
            throw new IllegalArgumentException("--mode must be 'valid' or 'invalid', got: " + mode);
        }
    }

    private static void produceValid(Properties props, Map<String, String> a) throws Exception {
        String topic = required(a, "topic");
        String apicurioUrl = required(a, "apicurio-url");
        String payloadFile = required(a, "payload-file");
        String payload = Files.readString(Path.of(payloadFile));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSchemaKafkaSerializer.class.getName());
        props.put(SerdeConfig.REGISTRY_URL, apicurioUrl);
        // Apicurio looks up subject "{topic}-value" by default with TopicIdStrategy.
        props.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getName());
        // Don't auto-register; the test script registers the schema explicitly.
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "false");
        // Validate the payload against the fetched schema before sending.
        props.put(SerdeConfig.VALIDATION_ENABLED, "true");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> f = producer.send(new ProducerRecord<>(topic, null, payload));
            RecordMetadata md = f.get();
            System.out.println("OK: produced valid record to " + md.topic()
                    + " partition=" + md.partition() + " offset=" + md.offset());
        }
    }

    private static void produceInvalid(Properties props, Map<String, String> a) throws Exception {
        String topic = required(a, "topic");
        String payloadFile = required(a, "payload-file");
        long globalId = Long.parseLong(required(a, "global-id"));
        byte[] payload = Files.readAllBytes(Path.of(payloadFile));

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // V3 envelope: 1 byte magic (0x00) + 8 bytes globalId (big-endian) + payload.
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeByte(0x00);
            dos.writeLong(globalId);
            dos.write(payload);
        }
        byte[] enveloped = bos.toByteArray();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            try {
                RecordMetadata md = producer.send(new ProducerRecord<>(topic, null, enveloped)).get();
                System.out.println("UNEXPECTED OK: invalid record was accepted at " + md.topic()
                        + " partition=" + md.partition() + " offset=" + md.offset());
                System.exit(2);
            } catch (Exception e) {
                String msg = e.getMessage() == null ? e.toString() : e.getMessage();
                System.out.println("REJECTED: " + msg);
                // Surface the chain so the test script can grep for INVALID_RECORD.
                Throwable t = e;
                while (t.getCause() != null && t.getCause() != t) {
                    t = t.getCause();
                    System.out.println("caused by: " + t);
                }
                System.exit(1);
            }
        }
    }

    private static Properties baseProducerProps(Map<String, String> a) {
        String bootstrap = required(a, "bootstrap");
        String keystore = required(a, "keystore");
        String truststore = required(a, "truststore");
        String keystorePass = a.getOrDefault("keystore-pass", "changeit");
        String truststorePass = a.getOrDefault("truststore-pass", "changeit");
        String tokenFile = required(a, "token-file");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "20000");

        // SASL_SSL with mTLS keystore + OAUTHBEARER.
        p.put("security.protocol", "SASL_SSL");
        p.put("ssl.keystore.type", "PKCS12");
        p.put("ssl.keystore.location", keystore);
        p.put("ssl.keystore.password", keystorePass);
        p.put("ssl.truststore.type", "PKCS12");
        p.put("ssl.truststore.location", truststore);
        p.put("ssl.truststore.password", truststorePass);
        p.put("sasl.mechanism", "OAUTHBEARER");
        p.put("sasl.oauthbearer.token.endpoint.url", "file://" + tokenFile);
        p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        p.put("sasl.login.callback.handler.class",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        return p;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a.startsWith("--")) {
                String key = a.substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    out.put(key, args[++i]);
                } else {
                    out.put(key, "true");
                }
            }
        }
        return out;
    }

    private static String required(Map<String, String> a, String key) {
        String v = a.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required --" + key);
        }
        return v;
    }
}
