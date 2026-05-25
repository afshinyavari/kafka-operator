package se.afshin.yavari.kafka.editor.run;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;

import se.afshin.yavari.kafka.editor.interpreter.SourceInfo;
import se.afshin.yavari.kafka.editor.model.RecordType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Feeds generated sample records into a live run's source topics on a timer. */
public final class TestDataProducer {

    private final KafkaProducer<String, byte[]> producer;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "kafka-editor-test-data");
                thread.setDaemon(true);
                return thread;
            });
    private final List<String> topics;
    private final Map<String, RecordType> recordTypeByTopic;
    private final List<RecordType> allRecordTypes;
    private final SampleDataGenerator generator;
    private final ObjectMapper mapper;
    private final AtomicInteger counter = new AtomicInteger();

    public TestDataProducer(
            String bootstrapServers,
            List<SourceInfo> sources,
            List<RecordType> recordTypes,
            SampleDataGenerator generator,
            ObjectMapper mapper) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-editor-test-data");
        this.producer = new KafkaProducer<>(props);
        this.generator = generator;
        this.mapper = mapper;
        this.allRecordTypes = recordTypes;

        Map<String, RecordType> byId = new HashMap<>();
        for (RecordType type : recordTypes) {
            byId.put(type.id(), type);
        }
        Map<String, RecordType> byTopic = new HashMap<>();
        Set<String> seen = new HashSet<>();
        for (SourceInfo source : sources) {
            if (seen.add(source.topicName())) {
                byTopic.put(source.topicName(), byId.get(source.recordTypeId()));
            }
        }
        this.recordTypeByTopic = byTopic;
        this.topics = List.copyOf(byTopic.keySet());
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::produceTick, 400, 250, TimeUnit.MILLISECONDS);
    }

    private void produceTick() {
        int index = counter.getAndIncrement();
        for (String topic : topics) {
            try {
                byte[] value = mapper.writeValueAsBytes(generator.generate(
                        recordTypeByTopic.get(topic), allRecordTypes, index));
                producer.send(new ProducerRecord<>(topic, "key-" + index, value));
            } catch (Exception ignored) {
                // A failed produce is non-fatal — the next tick retries.
            }
        }
    }

    public void close() {
        scheduler.shutdownNow();
        try {
            producer.close(Duration.ofSeconds(2));
        } catch (Exception ignored) {
            // best effort
        }
    }
}
