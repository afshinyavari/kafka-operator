package se.afshin.yavari.kafka.ui.messages;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;
import se.afshin.yavari.kafka.ui.serde.MessageDeserializer;
import se.afshin.yavari.kafka.ui.serde.MessageDeserializer.Side;
import se.afshin.yavari.kafka.ui.serde.Rendered;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.oidc.AccessTokenCredential;

/** Builds a paginated view of records and a streaming tail (SSE). */
@ApplicationScoped
public class MessageBrowserService {

    private static final Logger LOG = Logger.getLogger(MessageBrowserService.class);

    @Inject KafkaClientProvider clients;
    @Inject ClusterRegistry registry;
    @Inject MessageDeserializer deserializer;
    @Inject AccessTokenCredential userToken;

    @ConfigProperty(name = "kafka-ui.messages.max-page-size") int maxPageSize;
    @ConfigProperty(name = "kafka-ui.messages.poll-timeout-ms") long pollTimeoutMs;

    public enum SeekMode { OFFSET, TIMESTAMP, LATEST }

    public record SeekSpec(SeekMode mode, int partition, long value, int latestN) {}

    public record RenderedRecord(int partition, long offset, long timestamp,
                                 Rendered key, Rendered value) {}

    public record Page(String topic, int partition, long startOffset, long nextOffset,
                       boolean truncated, List<RenderedRecord> rows) {}

    public Page page(String clusterId, String topic, SeekSpec spec, int pageSize) {
        int size = Math.min(Math.max(pageSize, 1), maxPageSize);
        ClusterCoordinates coords = registry.byId(clusterId).orElseThrow();
        Consumer<byte[], byte[]> c = clients.consumer(clusterId);
        try {
            TopicPartition tp = new TopicPartition(topic, spec.partition());
            c.assign(List.of(tp));

            long startOffset;
            switch (spec.mode()) {
                case OFFSET -> { c.seek(tp, Math.max(spec.value(), 0)); startOffset = spec.value(); }
                case TIMESTAMP -> {
                    var found = c.offsetsForTimes(Map.of(tp, spec.value()));
                    long off = (found.get(tp) != null) ? found.get(tp).offset() : 0L;
                    c.seek(tp, off);
                    startOffset = off;
                }
                case LATEST -> {
                    Map<TopicPartition, Long> endMap = c.endOffsets(List.of(tp));
                    long end = endMap.getOrDefault(tp, 0L);
                    long from = Math.max(0, end - Math.max(1, spec.latestN()));
                    c.seek(tp, from);
                    startOffset = from;
                }
                default -> { startOffset = 0; }
            }

            ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(pollTimeoutMs));
            List<RenderedRecord> rows = new ArrayList<>();
            long next = startOffset;
            for (ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                if (rows.size() >= size) break;
                rows.add(renderRecord(coords, r));
                next = r.offset() + 1;
            }
            boolean truncated = recs.count() > rows.size();
            return new Page(topic, tp.partition(), startOffset, next, truncated, rows);
        } finally {
            try { c.close(Duration.ofSeconds(2)); } catch (Exception e) { LOG.debug("consumer close", e); }
        }
    }

    /**
     * Returns the partitions available on the topic. Used by the page to render
     * a partition picker.
     */
    public List<Integer> partitionsOf(String clusterId, String topic) {
        Consumer<byte[], byte[]> c = clients.consumer(clusterId);
        try {
            List<PartitionInfo> ps = c.partitionsFor(topic);
            List<Integer> out = new ArrayList<>();
            if (ps != null) for (PartitionInfo p : ps) out.add(p.partition());
            out.sort(Integer::compareTo);
            return out;
        } finally {
            try { c.close(Duration.ofSeconds(2)); } catch (Exception e) { /* ignore */ }
        }
    }

    /** SSE tail — emits HTML row fragments for new records on the chosen partitions. */
    public Multi<RenderedRecord> tail(String clusterId, String topic, Collection<Integer> partitions) {
        ClusterCoordinates coords = registry.byId(clusterId).orElseThrow();
        // Token captured up-front so the polling thread does not need request-scoped beans.
        String token = userToken.getToken();
        Consumer<byte[], byte[]> c = clients.consumer(clusterId);
        List<TopicPartition> tps = new ArrayList<>();
        for (int p : partitions) tps.add(new TopicPartition(topic, p));
        if (tps.isEmpty()) {
            for (PartitionInfo p : c.partitionsFor(topic)) tps.add(new TopicPartition(topic, p.partition()));
        }
        c.assign(tps);
        // Seek every assigned partition to the end so we only see fresh records.
        Map<TopicPartition, Long> end = c.endOffsets(tps);
        for (TopicPartition tp : tps) c.seek(tp, end.getOrDefault(tp, 0L));

        AtomicBoolean running = new AtomicBoolean(true);
        return Multi.createFrom().emitter(emitter -> {
            Thread worker = new Thread(() -> {
                try {
                    while (running.get()) {
                        ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(pollTimeoutMs));
                        for (ConsumerRecord<byte[], byte[]> r : recs) {
                            if (!running.get()) break;
                            emitter.emit(renderRecord(coords, r, token));
                        }
                    }
                    emitter.complete();
                } catch (Throwable t) {
                    if (running.get()) emitter.fail(t);
                } finally {
                    try { c.close(Duration.ofSeconds(2)); } catch (Exception e) { /* ignore */ }
                }
            }, "kafka-ui-tail-" + topic);
            worker.setDaemon(true);
            emitter.onTermination(() -> running.set(false));
            worker.start();
        });
    }

    private RenderedRecord renderRecord(ClusterCoordinates coords, ConsumerRecord<byte[], byte[]> r) {
        return renderRecord(coords, r, userToken.getToken());
    }

    private RenderedRecord renderRecord(ClusterCoordinates coords, ConsumerRecord<byte[], byte[]> r, String token) {
        Rendered key = deserializer.render(r.topic(), Side.KEY, r.key(), coords.apicurioUrl(), token);
        Rendered value = deserializer.render(r.topic(), Side.VALUE, r.value(), coords.apicurioUrl(), token);
        // Stub placeholders so the test still works if anyone passes a partial map
        if (key == null) key = Rendered.empty();
        if (value == null) value = Rendered.empty();
        return new RenderedRecord(r.partition(), r.offset(), r.timestamp(), key, value);
    }

}
