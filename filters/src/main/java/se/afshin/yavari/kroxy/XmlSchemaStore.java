package se.afshin.yavari.kroxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class XmlSchemaStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(XmlSchemaStore.class);

    private final String bootstrapServers;
    private final String schemaTopic;
    private final Properties securityProps;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<String, XmlValidator> validators = new ConcurrentHashMap<>();
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ExecutorService consumerThread = Executors.newSingleThreadExecutor(
        r -> new Thread(r, "xml-schema-store-consumer")
    );
    final ExecutorService validationExecutor;
    private KafkaProducer<String, String> producer;

    public XmlSchemaStore(String bootstrapServers, String schemaTopic, int validationThreadPoolSize) {
        this(bootstrapServers, schemaTopic, validationThreadPoolSize, new Properties());
    }

    public XmlSchemaStore(String bootstrapServers, String schemaTopic, int validationThreadPoolSize,
                          Properties securityProps) {
        this.bootstrapServers = bootstrapServers;
        this.schemaTopic = schemaTopic;
        this.securityProps = securityProps;
        this.validationExecutor = Executors.newFixedThreadPool(validationThreadPoolSize,
            r -> new Thread(r, "xml-validation-worker"));
    }

    public void start() throws InterruptedException {
        producer = createProducer();
        consumerThread.submit(this::runConsumer);
        readyLatch.await();
        log.info("XmlSchemaStore ready with {} schema(s)", validators.size());
    }

    private void runConsumer() {
        try (KafkaConsumer<String, String> consumer = createConsumer()) {
            TopicPartition partition = new TopicPartition(schemaTopic, 0);
            consumer.assign(Collections.singletonList(partition));
            consumer.seekToBeginning(Collections.singletonList(partition));

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(partition));
            long highWatermark = endOffsets.getOrDefault(partition, 0L);

            if (highWatermark == 0) {
                // Empty topic — ready immediately
                readyLatch.countDown();
            }

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    applyRecord(record.key(), record.value());
                    if (readyLatch.getCount() > 0 && record.offset() >= highWatermark - 1) {
                        readyLatch.countDown();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Schema consumer error, unblocking startup", e);
            readyLatch.countDown();
        }
    }

    private void applyRecord(String topic, String value) {
        if (value == null) {
            validators.remove(topic);
            log.info("Removed schema for topic '{}'", topic);
            return;
        }
        try {
            SchemaRecord record = objectMapper.readValue(value, SchemaRecord.class);
            validators.put(topic, new XmlValidator(record.getXsd()));
            log.info("Loaded schema for topic '{}'", topic);
        } catch (SAXException e) {
            log.error("Invalid XSD for topic '{}': {}", topic, e.getMessage());
        } catch (Exception e) {
            log.error("Failed to process schema record for topic '{}'", topic, e);
        }
    }

    public void publishSchema(SchemaRecord record) throws Exception {
        record.setUploadedAt(Instant.now().toString());
        String value = objectMapper.writeValueAsString(record);
        producer.send(new ProducerRecord<>(schemaTopic, record.getTopic(), value)).get();
    }

    public void deleteSchema(String topic) throws Exception {
        // Tombstone record triggers deletion on all instances via compacted topic
        producer.send(new ProducerRecord<>(schemaTopic, topic, null)).get();
    }

    public Optional<XmlValidator> getValidator(String topic) {
        return Optional.ofNullable(validators.get(topic));
    }

    public Map<String, String> listTopics() {
        Map<String, String> result = new HashMap<>();
        validators.keySet().forEach(k -> result.put(k, "registered"));
        return result;
    }

    @Override
    public void close() {
        running.set(false);
        consumerThread.shutdownNow();
        validationExecutor.shutdownNow();
        if (producer != null) {
            producer.close();
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Unique group ID so each instance independently replays the full topic
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kroxy-xml-schema-store-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putAll(securityProps);
        return new KafkaConsumer<>(props);
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.putAll(securityProps);
        return new KafkaProducer<>(props);
    }

    /**
     * Reads PEM cert/key/CA files from disk and returns the inline-PEM Kafka client
     * properties that point at them. Returns an empty Properties when {@code securityProtocol}
     * is null/PLAINTEXT (preserves the previous PLAINTEXT behavior). Throws if SSL is
     * requested but any path is missing or unreadable.
     */
    public static Properties buildSecurityProps(String securityProtocol,
                                                 String keystoreCertPath,
                                                 String keystoreKeyPath,
                                                 String truststoreCertPath) {
        Properties props = new Properties();
        if (securityProtocol == null || securityProtocol.isBlank()
                || "PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            return props;
        }
        if (!"SSL".equalsIgnoreCase(securityProtocol)) {
            throw new IllegalArgumentException(
                    "Only SSL (or PLAINTEXT) supported on XmlSchemaStore today, got: " + securityProtocol);
        }
        if (keystoreCertPath == null || keystoreKeyPath == null || truststoreCertPath == null) {
            throw new IllegalArgumentException(
                    "securityProtocol=SSL requires sslKeystoreCertPath, sslKeystoreKeyPath, sslTruststoreCertPath");
        }
        try {
            String cert = Files.readString(Path.of(keystoreCertPath));
            String key = Files.readString(Path.of(keystoreKeyPath));
            String ca = Files.readString(Path.of(truststoreCertPath));
            props.put("security.protocol", "SSL");
            props.put("ssl.keystore.type", "PEM");
            props.put("ssl.keystore.certificate.chain", cert);
            props.put("ssl.keystore.key", key);
            props.put("ssl.truststore.type", "PEM");
            props.put("ssl.truststore.certificates", ca);
            // Broker cert SAN uses the headless service hostname; disable strict hostname
            // checking so reaching the broker via cluster.local or pod-IP both work.
            props.put("ssl.endpoint.identification.algorithm", "");
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read PEM cert/key/CA for XmlSchemaStore", e);
        }
        return props;
    }
}
