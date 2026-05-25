package se.afshin.yavari.kroxy.audit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

/**
 * Factory for the process-wide {@link AuditEmitter}. Wires the always-on
 * stdout sink and (when {@code KAFKA_AUDIT_BOOTSTRAP} is set) the Kafka sink.
 *
 * <p>Environment contract — mirrors the proxy's existing PEM source-mode mTLS
 * pattern used by the XML validation filter and the broker INTERNAL listener:
 * <ul>
 *   <li>{@code KAFKA_AUDIT_BOOTSTRAP} — bootstrap.servers (e.g.
 *       {@code my-cluster-internal:9092}). When unset, only the stdout sink is
 *       installed and Kafka shipping is disabled.</li>
 *   <li>{@code KAFKA_AUDIT_TOPIC} — destination topic (default {@code __audit}).</li>
 *   <li>{@code KAFKA_AUDIT_TLS_CERT}, {@code KAFKA_AUDIT_TLS_KEY},
 *       {@code KAFKA_AUDIT_TLS_CA} — PEM file paths. When all three are set
 *       the producer connects over SSL with PEM source-mode (no PKCS12 step).</li>
 * </ul>
 */
public final class AuditEmitters {

    public static final String DEFAULT_TOPIC = "__audit";

    private AuditEmitters() {}

    /** Reads the {@code KAFKA_AUDIT_*} environment and returns the right emitter. */
    public static AuditEmitter fromEnv() {
        return fromEnv(System::getenv);
    }

    /** Test-visible variant taking an explicit env lookup. */
    static AuditEmitter fromEnv(EnvLookup env) {
        AuditEmitter stdout = new StdoutAuditEmitter();
        String bootstrap = env.get("KAFKA_AUDIT_BOOTSTRAP");
        if (bootstrap == null || bootstrap.isBlank()) {
            return stdout;
        }
        String topic = env.get("KAFKA_AUDIT_TOPIC");
        if (topic == null || topic.isBlank()) topic = DEFAULT_TOPIC;
        Properties props = producerProps(bootstrap,
                env.get("KAFKA_AUDIT_TLS_CERT"),
                env.get("KAFKA_AUDIT_TLS_KEY"),
                env.get("KAFKA_AUDIT_TLS_CA"));
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props,
                new ByteArraySerializer(), new ByteArraySerializer());
        AuditEmitter kafka = new KafkaAuditEmitter(producer, topic, stdout);
        return new CompositeAuditEmitter(List.of(stdout, kafka));
    }

    /** Producer config tuned for a never-block, fire-and-forget audit stream. */
    static Properties producerProps(String bootstrap, String certPath, String keyPath, String caPath) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-audit-emitter");
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        // Mandatory: never block the request thread. A full accumulator surfaces as a
        // synchronous BufferExhaustedException which KafkaAuditEmitter handles like any
        // other send failure (fall back to stdout, throttled warn).
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
        // delivery.timeout.ms >= linger.ms + request.timeout.ms (Kafka client validation).
        // request.timeout.ms defaults to 30s, linger=20, so 60s gives plenty of headroom.
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        if (certPath != null && keyPath != null && caPath != null
                && !certPath.isBlank() && !keyPath.isBlank() && !caPath.isBlank()) {
            p.put("security.protocol", "SSL");
            p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
            p.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, readPem(certPath));
            p.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, readPem(keyPath));
            p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            p.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, readPem(caPath));
        }
        return p;
    }

    private static String readPem(String path) {
        try {
            return Files.readString(Path.of(path));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read PEM file at " + path, e);
        }
    }

    @FunctionalInterface
    interface EnvLookup {
        String get(String key);
    }
}
