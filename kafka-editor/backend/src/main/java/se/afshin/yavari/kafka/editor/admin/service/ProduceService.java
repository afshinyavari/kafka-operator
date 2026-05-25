package se.afshin.yavari.kafka.editor.admin.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.AdminErrors;
import se.afshin.yavari.kafka.editor.admin.dto.ProduceAvroRequest;
import se.afshin.yavari.kafka.editor.admin.dto.ProduceRequest;
import se.afshin.yavari.kafka.editor.admin.dto.ReplayRequest;
import se.afshin.yavari.kafka.editor.admin.dto.SendResult;
import se.afshin.yavari.kafka.editor.admin.serde.Decoders;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/** Produces records, and replays existing ones byte-for-byte. */
@ApplicationScoped
public class ProduceService {

    @Inject
    AdminClientFactory factory;

    @Inject
    MessageBrowseService browseService;

    @Inject
    ObjectMapper mapper;

    /** Produce one record from the values entered in the UI. */
    public SendResult send(ConnectionConfig conn, ProduceRequest request) {
        if (request.topic() == null || request.topic().isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST", "Topic is required.");
        }
        byte[] key = request.key() == null || request.key().isEmpty()
                ? null
                : request.key().getBytes(StandardCharsets.UTF_8);
        byte[] value;
        if (request.tombstone()) {
            value = null;
        } else {
            String text = request.value() == null ? "" : request.value();
            if ("json".equalsIgnoreCase(request.valueType())) {
                try {
                    mapper.readTree(text);
                } catch (Exception e) {
                    throw new AdminApiException(400, "BAD_REQUEST",
                            "Value is not valid JSON: " + e.getMessage());
                }
            }
            value = text.getBytes(StandardCharsets.UTF_8);
        }
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                request.topic(), request.partition(), key, value);
        if (request.headers() != null) {
            request.headers().forEach((k, v) -> {
                if (k != null && !k.isBlank()) {
                    record.headers().add(k,
                            v == null ? null : v.getBytes(StandardCharsets.UTF_8));
                }
            });
        }
        return dispatch(conn, record);
    }

    /** Re-produce an existing record's raw bytes and headers to a target topic. */
    public SendResult replay(ConnectionConfig conn, ReplayRequest request) {
        if (request.sourceTopic() == null || request.partition() == null
                || request.offset() == null || request.targetTopic() == null
                || request.targetTopic().isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "sourceTopic, partition, offset and targetTopic are required.");
        }
        ConsumerRecord<byte[], byte[]> source = browseService.rawRecord(conn,
                request.sourceTopic(), request.partition(), request.offset());
        if (source == null) {
            throw new AdminApiException(404, "NOT_FOUND",
                    "No record at " + request.sourceTopic() + "-"
                            + request.partition() + "@" + request.offset());
        }
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                request.targetTopic(), null, source.key(), source.value());
        source.headers().forEach(h -> record.headers().add(h));
        return dispatch(conn, record);
    }

    /**
     * Produce a record whose value is JSON encoded against an Avro schema and
     * wrapped in the Apicurio envelope (0x00 + 8-byte global id).
     */
    public SendResult sendAvro(ConnectionConfig conn, ProduceAvroRequest request) {
        if (request.topic() == null || request.topic().isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST", "Topic is required.");
        }
        if (request.schema() == null || request.schema().isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "A schema is required for schema-aware produce.");
        }
        byte[] avro;
        try {
            String json = request.jsonValue() == null
                    || request.jsonValue().isBlank()
                    ? "null" : request.jsonValue();
            avro = Decoders.encodeAvro(request.schema(), json);
        } catch (Exception e) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Value does not match the schema: " + e.getMessage());
        }
        long globalId = request.globalId() == null ? 0L : request.globalId();
        ByteBuffer buffer = ByteBuffer.allocate(9 + avro.length);
        buffer.put((byte) 0).putLong(globalId).put(avro);
        byte[] key = request.key() == null || request.key().isEmpty()
                ? null
                : request.key().getBytes(StandardCharsets.UTF_8);
        return dispatch(conn, new ProducerRecord<>(request.topic(),
                request.partition(), key, buffer.array()));
    }

    private SendResult dispatch(ConnectionConfig conn,
            ProducerRecord<byte[], byte[]> record) {
        try {
            RecordMetadata md = factory.producer(conn.bootstrapServersOrDefault())
                    .send(record).get(10, TimeUnit.SECONDS);
            return new SendResult(md.topic(), md.partition(), md.offset(),
                    md.timestamp());
        } catch (ExecutionException e) {
            throw AdminErrors.translate(
                    e.getCause() != null ? e.getCause() : e);
        } catch (Exception e) {
            throw AdminErrors.translate(e);
        }
    }
}
