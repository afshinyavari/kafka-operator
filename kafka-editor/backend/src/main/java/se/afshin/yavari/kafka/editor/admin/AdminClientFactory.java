package se.afshin.yavari.kafka.editor.admin;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Builds Kafka clients for the admin layer.
 *
 * <p>{@link Admin} and {@link KafkaProducer} are thread-safe and are cached one
 * per bootstrap-servers string. {@link KafkaConsumer} is <em>not</em>
 * thread-safe: a fresh one is created per call and the caller must close it.
 *
 * <p>Local-first: the editor talks to a handful of clusters at most, so plain
 * maps closed on shutdown are sufficient — no eviction policy is needed.
 */
@ApplicationScoped
public class AdminClientFactory {

    static final int TIMEOUT_MS = 15_000;
    private static final int FETCH_MAX_BYTES = 4 * 1024 * 1024;

    private final Map<String, Admin> adminClients = new ConcurrentHashMap<>();
    private final Map<String, KafkaProducer<byte[], byte[]>> producers =
            new ConcurrentHashMap<>();

    /** A cached admin client for the connection's bootstrap servers. */
    public Admin admin(ConnectionConfig connection) {
        return admin(connection.bootstrapServersOrDefault());
    }

    /** A cached admin client for the given bootstrap servers. */
    public Admin admin(String bootstrapServers) {
        return adminClients.computeIfAbsent(
                normalize(bootstrapServers),
                key -> Admin.create(adminProps(key)));
    }

    /** A cached, thread-safe byte-array producer. */
    public KafkaProducer<byte[], byte[]> producer(String bootstrapServers) {
        return producers.computeIfAbsent(
                normalize(bootstrapServers),
                key -> new KafkaProducer<>(producerProps(key)));
    }

    /**
     * A fresh byte-array consumer with a unique group id and no auto-commit.
     * Consumers are not thread-safe — the caller owns and must close it.
     */
    public KafkaConsumer<byte[], byte[]> newConsumer(String bootstrapServers) {
        return new KafkaConsumer<>(consumerProps(normalize(bootstrapServers)));
    }

    /**
     * A quick reachability probe — {@code null} if the broker answered, an
     * error message otherwise.
     */
    public String probe(String bootstrapServers) {
        try {
            admin(bootstrapServers).listTopics().names()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return null;
        } catch (Exception e) {
            return "Could not reach the Kafka broker at " + bootstrapServers
                    + ". Is it running? (docker compose up -d)";
        }
    }

    static Properties adminProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        // The total budget for an operation, retries included. Multi-step calls
        // (e.g. listConsumerGroups) need retries within this window, so we do
        // NOT pin retries to 0 — DEFAULT_API_TIMEOUT_MS bounds the wait.
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-editor-admin");
        return props;
    }

    private static Properties consumerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-editor-browse-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, FETCH_MAX_BYTES);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        return props;
    }

    private static Properties producerProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        return props;
    }

    static String normalize(String bootstrapServers) {
        return bootstrapServers == null || bootstrapServers.isBlank()
                ? "localhost:9092"
                : bootstrapServers.trim();
    }

    @PreDestroy
    void shutdown() {
        adminClients.values().forEach(admin -> {
            try {
                admin.close(Duration.ofSeconds(2));
            } catch (Exception ignored) {
                // best effort on shutdown
            }
        });
        producers.values().forEach(producer -> {
            try {
                producer.close(Duration.ofSeconds(2));
            } catch (Exception ignored) {
                // best effort on shutdown
            }
        });
        adminClients.clear();
        producers.clear();
    }
}
