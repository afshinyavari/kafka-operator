package se.afshin.yavari.kafka.editor.admin.service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.dto.HeaderKv;
import se.afshin.yavari.kafka.editor.admin.dto.MessagePage;
import se.afshin.yavari.kafka.editor.admin.dto.RenderedRecord;
import se.afshin.yavari.kafka.editor.admin.dto.SeekMode;
import se.afshin.yavari.kafka.editor.admin.dto.SearchResult;
import se.afshin.yavari.kafka.editor.admin.serde.MessageDeserializer;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

/** Browses, tails, searches and replays records on a topic. */
@ApplicationScoped
public class MessageBrowseService {

    private static final int MAX_PAGE = 500;
    private static final int MAX_SCAN = 5_000;
    private static final int MAX_MATCHES = 200;

    @Inject
    AdminClientFactory factory;

    @Inject
    MessageDeserializer deserializer;

    /** Partition ids of a topic. */
    public List<Integer> partitionsOf(ConnectionConfig conn, String topic) {
        try (KafkaConsumer<byte[], byte[]> consumer =
                factory.newConsumer(conn.bootstrapServersOrDefault())) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic);
            if (infos == null || infos.isEmpty()) {
                throw new AdminApiException(404, "NOT_FOUND",
                        "No such topic: " + topic);
            }
            return infos.stream().map(PartitionInfo::partition).sorted().toList();
        }
    }

    /** One page of records from a single partition. */
    public MessagePage page(ConnectionConfig conn, String topic, int partition,
            SeekMode mode, Long offset, Long timestamp, int size) {
        int limit = Math.min(Math.max(size, 1), MAX_PAGE);
        String registry = conn.schemaRegistryUrlOrNull();
        try (KafkaConsumer<byte[], byte[]> consumer =
                factory.newConsumer(conn.bootstrapServersOrDefault())) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            long begin = consumer.beginningOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long start = resolveStart(consumer, tp, mode, offset, timestamp,
                    begin, end, limit);
            if (start >= end) {
                return new MessagePage(topic, partition, begin, end, end, false,
                        List.of());
            }
            consumer.seek(tp, start);
            List<RenderedRecord> rows = new ArrayList<>();
            long nextOffset = start;
            long deadline = System.currentTimeMillis() + 5_000;
            while (rows.size() < limit && nextOffset < end
                    && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> polled =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> rec : polled.records(tp)) {
                    if (rows.size() >= limit) {
                        break;
                    }
                    rows.add(render(rec, registry));
                    nextOffset = rec.offset() + 1;
                }
            }
            return new MessagePage(topic, partition, begin, end, nextOffset,
                    nextOffset < end, rows);
        }
    }

    /** Bounded server-side scan of a partition's recent records. */
    public SearchResult search(ConnectionConfig conn, String topic, int partition,
            String query, String field, boolean caseSensitive, int scanLimit) {
        if (query == null || query.isEmpty()) {
            throw new AdminApiException(400, "BAD_REQUEST", "A query is required.");
        }
        int scanCap = Math.min(Math.max(scanLimit, 1), MAX_SCAN);
        String needle = caseSensitive ? query : query.toLowerCase();
        String registry = conn.schemaRegistryUrlOrNull();
        try (KafkaConsumer<byte[], byte[]> consumer =
                factory.newConsumer(conn.bootstrapServersOrDefault())) {
            TopicPartition tp = new TopicPartition(topic, partition);
            consumer.assign(List.of(tp));
            long begin = consumer.beginningOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
            long start = Math.max(begin, end - scanCap);
            if (start >= end) {
                return new SearchResult(0, false, List.of());
            }
            consumer.seek(tp, start);
            List<RenderedRecord> matches = new ArrayList<>();
            int scanned = 0;
            long nextOffset = start;
            long deadline = System.currentTimeMillis() + 10_000;
            while (nextOffset < end && scanned < scanCap
                    && matches.size() < MAX_MATCHES
                    && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> polled =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> rec : polled.records(tp)) {
                    if (scanned >= scanCap || matches.size() >= MAX_MATCHES) {
                        break;
                    }
                    scanned++;
                    nextOffset = rec.offset() + 1;
                    RenderedRecord rendered = render(rec, registry);
                    if (matches(rendered, needle, field, caseSensitive)) {
                        matches.add(rendered);
                    }
                }
            }
            return new SearchResult(scanned, nextOffset < end, matches);
        }
    }

    /** A live stream of new records on the topic's partitions (SSE). */
    public Multi<RenderedRecord> tail(ConnectionConfig conn, String topic,
            List<Integer> partitions) {
        String bootstrap = conn.bootstrapServersOrDefault();
        String registry = conn.schemaRegistryUrlOrNull();
        return Multi.createFrom().emitter(emitter -> {
            AtomicBoolean running = new AtomicBoolean(true);
            emitter.onTermination(() -> running.set(false));
            Thread worker = new Thread(() -> {
                KafkaConsumer<byte[], byte[]> consumer = null;
                try {
                    consumer = factory.newConsumer(bootstrap);
                    List<TopicPartition> tps =
                            tailPartitions(consumer, topic, partitions);
                    consumer.assign(tps);
                    consumer.seekToEnd(tps);
                    while (running.get()) {
                        ConsumerRecords<byte[], byte[]> polled =
                                consumer.poll(Duration.ofMillis(500));
                        for (ConsumerRecord<byte[], byte[]> rec : polled) {
                            if (!running.get()) {
                                break;
                            }
                            emitter.emit(render(rec, registry));
                        }
                    }
                } catch (Exception e) {
                    // broker gone or consumer closed — end the stream below
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close(Duration.ofSeconds(1));
                        } catch (Exception ignored) {
                            // best effort
                        }
                    }
                    emitter.complete();
                }
            }, "kafka-tail-" + topic);
            worker.setDaemon(true);
            worker.start();
        }, BackPressureStrategy.DROP);
    }

    /** Re-produce the raw record at (sourceTopic, partition, offset). */
    public RenderedRecord readRaw(ConnectionConfig conn, String topic,
            int partition, long offset) {
        try (KafkaConsumer<byte[], byte[]> consumer =
                factory.newConsumer(conn.bootstrapServersOrDefault())) {
            ConsumerRecord<byte[], byte[]> rec = seekOne(consumer, topic, partition,
                    offset);
            if (rec == null) {
                throw new AdminApiException(404, "NOT_FOUND",
                        "No record at " + topic + "-" + partition + "@" + offset);
            }
            return render(rec, conn.schemaRegistryUrlOrNull());
        }
    }

    /** The raw consumer record at a single offset, or null if not present. */
    public ConsumerRecord<byte[], byte[]> rawRecord(ConnectionConfig conn,
            String topic, int partition, long offset) {
        try (KafkaConsumer<byte[], byte[]> consumer =
                factory.newConsumer(conn.bootstrapServersOrDefault())) {
            return seekOne(consumer, topic, partition, offset);
        }
    }

    private ConsumerRecord<byte[], byte[]> seekOne(
            KafkaConsumer<byte[], byte[]> consumer, String topic, int partition,
            long offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(List.of(tp));
        long end = consumer.endOffsets(List.of(tp)).getOrDefault(tp, 0L);
        if (offset < 0 || offset >= end) {
            return null;
        }
        consumer.seek(tp, offset);
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> polled =
                    consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<byte[], byte[]> rec : polled.records(tp)) {
                if (rec.offset() == offset) {
                    return rec;
                }
                if (rec.offset() > offset) {
                    return null;
                }
            }
        }
        return null;
    }

    private long resolveStart(KafkaConsumer<byte[], byte[]> consumer,
            TopicPartition tp, SeekMode mode, Long offset, Long timestamp,
            long begin, long end, int limit) {
        return switch (mode == null ? SeekMode.LATEST : mode) {
            case OFFSET -> offset == null
                    ? begin
                    : Math.min(Math.max(offset, begin), end);
            case TIMESTAMP -> {
                if (timestamp == null) {
                    yield end;
                }
                OffsetAndTimestamp at = consumer
                        .offsetsForTimes(Map.of(tp, timestamp)).get(tp);
                yield at == null ? end : at.offset();
            }
            case LATEST -> Math.max(begin, end - limit);
        };
    }

    private List<TopicPartition> tailPartitions(
            KafkaConsumer<byte[], byte[]> consumer, String topic,
            List<Integer> partitions) {
        if (partitions != null && !partitions.isEmpty()) {
            return partitions.stream()
                    .map(p -> new TopicPartition(topic, p))
                    .toList();
        }
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        if (infos == null) {
            return List.of();
        }
        return infos.stream()
                .map(i -> new TopicPartition(topic, i.partition()))
                .toList();
    }

    private RenderedRecord render(ConsumerRecord<byte[], byte[]> rec,
            String registryUrl) {
        List<HeaderKv> headers = new ArrayList<>();
        for (Header h : rec.headers()) {
            headers.add(new HeaderKv(h.key(),
                    h.value() == null
                            ? null
                            : new String(h.value(), StandardCharsets.UTF_8)));
        }
        return new RenderedRecord(rec.partition(), rec.offset(), rec.timestamp(),
                deserializer.render(rec.key(), registryUrl),
                deserializer.render(rec.value(), registryUrl),
                headers);
    }

    private static boolean matches(RenderedRecord rec, String needle, String field,
            boolean caseSensitive) {
        String scope = field == null ? "all" : field.toLowerCase();
        return switch (scope) {
            case "key" -> contains(rec.key().text(), needle, caseSensitive);
            case "value" -> contains(rec.value().text(), needle, caseSensitive);
            case "header" -> rec.headers().stream().anyMatch(h ->
                    contains(h.key(), needle, caseSensitive)
                            || contains(h.value(), needle, caseSensitive));
            default -> contains(rec.key().text(), needle, caseSensitive)
                    || contains(rec.value().text(), needle, caseSensitive)
                    || rec.headers().stream().anyMatch(h ->
                            contains(h.key(), needle, caseSensitive)
                                    || contains(h.value(), needle, caseSensitive));
        };
    }

    private static boolean contains(String text, String needle,
            boolean caseSensitive) {
        if (text == null) {
            return false;
        }
        return (caseSensitive ? text : text.toLowerCase()).contains(needle);
    }
}
