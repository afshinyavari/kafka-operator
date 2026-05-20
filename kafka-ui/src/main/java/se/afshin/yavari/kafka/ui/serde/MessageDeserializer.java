package se.afshin.yavari.kafka.ui.serde;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Decides how to render a Kafka record's bytes without the user having to pick
 * a deserializer. See {@code project_kafkaproxy_plan} / the design doc for the
 * full algorithm.
 *
 * <h3>Detection order</h3>
 * <ol>
 *   <li>{@code null} payload → {@link Rendered#tombstone()}</li>
 *   <li>{@code length == 0} → {@link Rendered#empty()}</li>
 *   <li>Apicurio V3 envelope ({@code 0x00 || globalId:int64 || payload}) — fetch
 *       schema from Apicurio, decode according to type (AVRO/PROTOBUF/JSON)</li>
 *   <li>Confluent envelope ({@code 0x00 || schemaId:int32 || payload}) — same,
 *       via the {@code /contentIds/} endpoint</li>
 *   <li>Topic-name convention ({@code {topic}-{side}} artifact in Apicurio) —
 *       decode the entire payload (no envelope) by the artifact's type</li>
 *   <li>Plain JSON heuristic (starts with {@code &#123;} or {@code [})</li>
 *   <li>UTF-8 heuristic (≥95% printable)</li>
 *   <li>Hex-dump fallback (first 256 bytes)</li>
 * </ol>
 */
@ApplicationScoped
public class MessageDeserializer {

    private static final Logger LOG = Logger.getLogger(MessageDeserializer.class);

    public enum Side { KEY, VALUE }

    @Inject SchemaCache cache;

    public Rendered render(String topic, Side side, byte[] bytes,
                           String apicurioUrl, String bearerToken) {
        if (bytes == null) return Rendered.tombstone();
        if (bytes.length == 0) return Rendered.empty();

        List<String> warnings = new ArrayList<>();

        // (3) Apicurio V3 envelope
        if (bytes[0] == 0x00 && bytes.length >= 9) {
            long gid = ByteBuffer.wrap(bytes, 1, 8).getLong();
            Optional<SchemaCache.SchemaMeta> meta = cache.byGlobalId(gid, apicurioUrl, bearerToken);
            if (meta.isPresent()) {
                byte[] payload = sliceFrom(bytes, 9);
                Rendered r = decode(meta.get(), payload, "APICURIO", String.valueOf(gid), warnings);
                if (r != null) return r;
                warnings.add("Apicurio:" + gid + " decode failed; falling through");
            } else {
                warnings.add("Apicurio: schema globalId=" + gid + " not found");
            }
        }

        // (4) Confluent envelope (same magic, 4-byte id)
        if (bytes[0] == 0x00 && bytes.length >= 5) {
            int cid = ByteBuffer.wrap(bytes, 1, 4).getInt();
            // Confluent contentIds map to Apicurio contentIds via apicurio's compat endpoint.
            // We reuse byGlobalId(...) here since most Apicurio deployments expose the same
            // id space; if the lookup misses we fall through.
            Optional<SchemaCache.SchemaMeta> meta = cache.byGlobalId(cid, apicurioUrl, bearerToken);
            if (meta.isPresent()) {
                byte[] payload = sliceFrom(bytes, 5);
                Rendered r = decode(meta.get(), payload, "CONFLUENT", String.valueOf(cid), warnings);
                if (r != null) return r;
            }
        }

        // (5) Topic-name convention
        String artifact = topic + "-" + (side == Side.KEY ? "key" : "value");
        Optional<SchemaCache.SchemaMeta> conv = cache.byArtifact(artifact, apicurioUrl, bearerToken);
        if (conv.isPresent()) {
            Rendered r = decode(conv.get(), bytes, "TOPIC_CONV", artifact, warnings);
            if (r != null) return r;
            warnings.add("TopicConv:" + artifact + " decode failed; falling through");
        }

        // (6) Plain JSON heuristic
        byte first = bytes[0];
        if (first == '{' || first == '[') {
            try {
                String pretty = Decoders.prettyJson(bytes);
                return ok("JSON", null, pretty, bytes.length, warnings);
            } catch (Exception ignored) {
                warnings.add("JSON parse failed");
            }
        }

        // (7) UTF-8 heuristic
        if (Decoders.isLikelyUtf8(bytes)) {
            return ok("STRING", null, Decoders.utf8(bytes), bytes.length, warnings);
        }

        // (8) Hex fallback
        return ok("BINARY", null, Decoders.hexDump(bytes),
                bytes.length, bytes.length > 256, warnings);
    }

    private Rendered decode(SchemaCache.SchemaMeta meta, byte[] payload, String strategy,
                            String ref, List<String> warnings) {
        try {
            switch (meta.type().toUpperCase()) {
                case "AVRO" -> {
                    String text = Decoders.decodeAvro(meta.content(), payload);
                    return ok(strategy, ref, text, payload.length, warnings);
                }
                case "JSON" -> {
                    // For JSON Schema artifacts, the payload is just JSON (no envelope-after-magic).
                    String text = Decoders.prettyJson(payload);
                    return ok(strategy, ref, text, payload.length, warnings);
                }
                case "PROTOBUF" -> {
                    warnings.add("Protobuf decoding lands in Phase 2 — showing hex");
                    return ok(strategy, ref, Decoders.hexDump(payload),
                            payload.length, payload.length > 256, warnings);
                }
                default -> {
                    warnings.add("Unknown schema type: " + meta.type());
                    return null;
                }
            }
        } catch (Exception e) {
            LOG.debugf(e, "decode(%s) failed", meta.type());
            warnings.add(strategy + " decode error: " + e.getMessage());
            return null;
        }
    }

    private static Rendered ok(String strategy, String ref, String text, int size, List<String> warnings) {
        return ok(strategy, ref, text, size, false, warnings);
    }

    private static Rendered ok(String strategy, String ref, String text,
                               int size, boolean truncated, List<String> warnings) {
        return new Rendered(strategy, ref, text, size, truncated, List.copyOf(warnings));
    }

    private static byte[] sliceFrom(byte[] src, int start) {
        byte[] out = new byte[src.length - start];
        System.arraycopy(src, start, out, 0, out.length);
        return out;
    }
}
