package se.afshin.yavari.kafka.editor.interpreter;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A registry-aware value Serde for live-run source topics. It detects the
 * record format and produces a `JsonNode` for the interpreter:
 *
 * <ul>
 *   <li>A leading 0x00 magic byte → registry-encoded; the next 8 bytes are an
 *       Apicurio global id. If it resolves to an Avro schema, the payload is
 *       Avro-decoded; otherwise the payload is treated as JSON.</li>
 *   <li>No magic byte → plain JSON.</li>
 * </ul>
 *
 * Serialization falls back to plain JSON (sinks write JSON — Avro write-back
 * is later work). Targets Apicurio's native 8-byte id format.
 */
public final class RegistryAvroSerde implements Serde<JsonNode> {

    private static final int HEADER_LENGTH = 9; // 1 magic byte + 8-byte id

    private final ObjectMapper mapper;
    private final String registryBase;
    private final JsonNodeSerde jsonSerde;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<Long, Optional<Schema>> schemaCache = new ConcurrentHashMap<>();

    public RegistryAvroSerde(ObjectMapper mapper, String registryUrl) {
        this.mapper = mapper;
        this.jsonSerde = new JsonNodeSerde(mapper);
        String trimmed = registryUrl == null ? "" : registryUrl.trim();
        this.registryBase = trimmed.endsWith("/")
                ? trimmed.substring(0, trimmed.length() - 1)
                : trimmed;
    }

    @Override
    public Serializer<JsonNode> serializer() {
        return jsonSerde.serializer();
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            if (bytes[0] != 0 || bytes.length < HEADER_LENGTH) {
                return parseJson(bytes, 0, bytes.length);
            }
            long globalId = ByteBuffer.wrap(bytes, 1, 8).getLong();
            Optional<Schema> schema = schemaFor(globalId);
            if (schema.isEmpty()) {
                // Not Avro (e.g. JSON Schema) — the payload after the header is JSON.
                return parseJson(bytes, HEADER_LENGTH, bytes.length - HEADER_LENGTH);
            }
            try {
                GenericDatumReader<Object> reader =
                        new GenericDatumReader<>(schema.get());
                Decoder decoder = DecoderFactory.get().binaryDecoder(
                        bytes, HEADER_LENGTH, bytes.length - HEADER_LENGTH, null);
                Object value = reader.read(null, decoder);
                if (value instanceof GenericRecord) {
                    return mapper.readTree(value.toString());
                }
                return mapper.valueToTree(value == null ? null : value.toString());
            } catch (Exception e) {
                return parseJson(bytes, HEADER_LENGTH, bytes.length - HEADER_LENGTH);
            }
        };
    }

    private JsonNode parseJson(byte[] bytes, int offset, int length) {
        try {
            return mapper.readTree(Arrays.copyOfRange(bytes, offset, offset + length));
        } catch (Exception e) {
            return NullNode.getInstance();
        }
    }

    private Optional<Schema> schemaFor(long globalId) {
        return schemaCache.computeIfAbsent(globalId, id -> {
            try {
                HttpResponse<String> response = http.send(
                        HttpRequest.newBuilder(URI.create(
                                registryBase + "/apis/registry/v2/ids/globalIds/" + id))
                                .GET()
                                .timeout(Duration.ofSeconds(8))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    return Optional.empty();
                }
                return Optional.of(new Schema.Parser().parse(response.body()));
            } catch (Exception e) {
                return Optional.empty();
            }
        });
    }
}
