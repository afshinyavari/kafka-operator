package se.afshin.yavari.kafka.editor.run;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.interpreter.InterpretedTopology;
import se.afshin.yavari.kafka.editor.interpreter.JsonNodeSerde;
import se.afshin.yavari.kafka.editor.interpreter.SourceInfo;
import se.afshin.yavari.kafka.editor.interpreter.TopologyInterpreter;
import se.afshin.yavari.kafka.editor.model.ProjectDocument;
import se.afshin.yavari.kafka.editor.model.RecordType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * Runs interpreted topologies — test mode via TopologyTestDriver (R1) and live
 * mode via a real KafkaStreams instance against a broker (R2).
 */
@ApplicationScoped
public class RunService {

    @Inject
    TopologyInterpreter interpreter;

    @Inject
    SampleDataGenerator sampleGenerator;

    @Inject
    ObjectMapper mapper;

    @Inject
    AdminClientFactory adminFactory;

    /** Active live runs, keyed by run id. */
    private final Map<String, RunInstance> runs = new ConcurrentHashMap<>();

    /**
     * Build and drive a topology with generated records, returning per-node
     * counts. Synchronous, in-memory — no Kafka broker involved.
     */
    public RunResult runTest(ProjectDocument doc, int recordsPerSource) {
        String runId = UUID.randomUUID().toString();
        if (doc == null || doc.nodes().isEmpty()) {
            return new RunResult(runId, "test", Map.of(), "The topology is empty.");
        }

        MetricsRegistry metrics = new MetricsRegistry();
        try {
            InterpretedTopology interpreted = interpreter.build(doc, metrics, null);
            Properties props = baseStreamsProps("kafka-editor-test", "dummy:9092");

            JsonNodeSerde valueSerde = new JsonNodeSerde(mapper);
            Map<String, RecordType> recordTypesById = new HashMap<>();
            for (RecordType type : doc.recordTypes()) {
                recordTypesById.put(type.id(), type);
            }

            try (TopologyTestDriver driver =
                    new TopologyTestDriver(interpreted.topology(), props)) {
                Set<String> piped = new HashSet<>();
                for (SourceInfo source : interpreted.sources()) {
                    if (!piped.add(source.topicName())) {
                        continue;
                    }
                    TestInputTopic<String, JsonNode> input = driver.createInputTopic(
                            source.topicName(),
                            Serdes.String().serializer(),
                            valueSerde.serializer());
                    RecordType type = recordTypesById.get(source.recordTypeId());
                    for (int i = 0; i < recordsPerSource; i++) {
                        input.pipeInput("key-" + i,
                                sampleGenerator.generate(type, doc.recordTypes(), i));
                    }
                }
            }
            return new RunResult(runId, "test", metrics.snapshot(), null);
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.toString();
            return new RunResult(runId, "test", metrics.snapshot(), message);
        }
    }

    /**
     * Start a live run — a real KafkaStreams instance against the broker.
     * Returns immediately; metrics are then streamed via {@link #snapshot}.
     */
    public RunResult runLive(
            ProjectDocument doc,
            String bootstrapServers,
            String registryUrl,
            Map<String, String> envVars,
            boolean generateInput) {
        String runId = UUID.randomUUID().toString();
        if (doc == null || doc.nodes().isEmpty()) {
            return new RunResult(runId, "live", Map.of(), "The topology is empty.");
        }
        String unreachable = adminFactory.probe(bootstrapServers);
        if (unreachable != null) {
            return new RunResult(runId, "live", Map.of(), unreachable);
        }

        MetricsRegistry metrics = new MetricsRegistry();
        try {
            InterpretedTopology interpreted =
                    interpreter.build(doc, metrics, registryUrl);
            // KafkaStreams fails fast if a source topic is missing — create
            // every topic the topology uses before starting.
            ensureTopics(interpreted.topics(), bootstrapServers);
            Properties props =
                    baseStreamsProps("kafka-editor-run-" + runId, bootstrapServers);
            props.put(StreamsConfig.STATE_DIR_CONFIG,
                    System.getProperty("java.io.tmpdir") + "/kafka-editor-" + runId);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
            props.put(
                    StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                    "earliest");
            // Environment variables become extra client config — the seam for
            // SASL/TLS credentials.
            if (envVars != null) {
                envVars.forEach((key, value) -> {
                    if (key != null && !key.isBlank()) {
                        props.put(key, value);
                    }
                });
            }

            KafkaStreams streams = new KafkaStreams(interpreted.topology(), props);
            streams.start();

            TestDataProducer producer = null;
            if (generateInput && !interpreted.sources().isEmpty()) {
                producer = new TestDataProducer(bootstrapServers, interpreted.sources(),
                        doc.recordTypes(), sampleGenerator, mapper);
                producer.start();
            }
            runs.put(runId, new RunInstance(runId, streams, metrics, producer));
            return new RunResult(runId, "live", Map.of(), null);
        } catch (Exception e) {
            String message = e.getMessage() != null ? e.getMessage() : e.toString();
            return new RunResult(runId, "live", Map.of(), message);
        }
    }

    /** A current view of a live run, or a "stopped" snapshot if it is gone. */
    public MetricsSnapshot snapshot(String runId) {
        RunInstance instance = runs.get(runId);
        if (instance == null) {
            return new MetricsSnapshot(runId, "stopped", Map.of());
        }
        return new MetricsSnapshot(runId, instance.status(),
                instance.metrics().snapshot());
    }

    /** Stop a live run. Returns false if no such run is active. */
    public boolean stop(String runId) {
        RunInstance instance = runs.remove(runId);
        if (instance == null) {
            return false;
        }
        instance.close();
        return true;
    }

    @PreDestroy
    void shutdown() {
        runs.values().forEach(RunInstance::close);
        runs.clear();
    }

    /** Create any of the topology's topics that do not yet exist on the broker. */
    private void ensureTopics(List<String> topicNames, String bootstrapServers) {
        if (topicNames == null || topicNames.isEmpty()) {
            return;
        }
        try {
            Admin admin = adminFactory.admin(bootstrapServers);
            Set<String> existing =
                    admin.listTopics().names().get(10, TimeUnit.SECONDS);
            List<NewTopic> toCreate = new ArrayList<>();
            for (String name : topicNames) {
                if (!existing.contains(name)) {
                    toCreate.add(new NewTopic(name, 1, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            // Topics may have been created concurrently — non-fatal.
        }
    }

    private Properties baseStreamsProps(String appId, String bootstrapServers) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                JsonNodeSerde.class.getName());
        return props;
    }
}
