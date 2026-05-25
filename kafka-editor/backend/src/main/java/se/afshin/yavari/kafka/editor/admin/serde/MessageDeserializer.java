package se.afshin.yavari.kafka.editor.admin.serde;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Renders a Kafka record's raw bytes without the user picking a deserializer.
 *
 * <p>Detection order: null -> tombstone; empty -> empty; a {@code 0x00} magic
 * byte -> a registry envelope (Apicurio 8-byte global id, then Confluent 4-byte
 * id) decoded as Avro; then a JSON heuristic; then a UTF-8 heuristic; then a
 * hex-dump fallback.
 */
@ApplicationScoped
public class MessageDeserializer {

    @Inject
    SchemaCache cache;

    public Rendered render(byte[] bytes, String registryUrl) {
        if (bytes == null) {
            return Rendered.tombstone();
        }
        if (bytes.length == 0) {
            return Rendered.empty();
        }
        List<String> warnings = new ArrayList<>();

        if (bytes[0] == 0x00) {
            if (bytes.length >= 9) {
                Rendered r = tryRegistry(bytes, 9,
                        ByteBuffer.wrap(bytes, 1, 8).getLong(), registryUrl);
                if (r != null) {
                    return r;
                }
            }
            if (bytes.length >= 5) {
                Rendered r = tryRegistry(bytes, 5,
                        ByteBuffer.wrap(bytes, 1, 4).getInt() & 0xffffffffL,
                        registryUrl);
                if (r != null) {
                    return r;
                }
            }
            if (registryUrl != null && !registryUrl.isBlank()) {
                warnings.add("Looks registry-encoded but the schema was not found.");
            }
        }

        byte first = bytes[0];
        if (first == '{' || first == '[') {
            try {
                return new Rendered("JSON", null, Decoders.prettyJson(bytes),
                        bytes.length, false, warnings);
            } catch (Exception ignored) {
                warnings.add("Looked like JSON but did not parse.");
            }
        }
        if (Decoders.isLikelyUtf8(bytes)) {
            return new Rendered("STRING", null, Decoders.utf8(bytes),
                    bytes.length, false, warnings);
        }
        return new Rendered("BINARY", null, Decoders.hexDump(bytes),
                bytes.length, bytes.length > 256, warnings);
    }

    /** Attempt an Avro decode against a registry schema; null = not this format. */
    private Rendered tryRegistry(byte[] bytes, int offset, long id,
            String registryUrl) {
        Optional<String> schema = cache.byId(registryUrl, id);
        if (schema.isEmpty()) {
            return null;
        }
        try {
            byte[] payload = Arrays.copyOfRange(bytes, offset, bytes.length);
            String text = Decoders.decodeAvro(schema.get(), payload);
            return new Rendered("AVRO", String.valueOf(id), text,
                    payload.length, false, List.of());
        } catch (Exception e) {
            return null;
        }
    }
}
