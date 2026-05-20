package se.afshin.yavari.kafka.ui.serde;

import java.util.List;

/**
 * Result of running a record's bytes through the smart deserializer.
 *
 * @param strategy   short tag matched by the detection algorithm; used as a
 *                   badge in the UI (e.g. {@code APICURIO}, {@code CONFLUENT},
 *                   {@code JSON}, {@code STRING}, {@code BINARY}, {@code TOMBSTONE}).
 * @param schemaRef  artifact id or global id, when one was matched; null otherwise.
 * @param text       human-readable rendering of the payload (pretty JSON, decoded
 *                   string, or hex dump). Empty when {@code strategy} is
 *                   {@code TOMBSTONE} or {@code EMPTY}.
 * @param sizeBytes  size of the original record bytes (helpful when {@code text}
 *                   is truncated).
 * @param truncated  true when {@code text} is shorter than the full payload.
 * @param warnings   non-fatal issues encountered (e.g. "Apicurio: schema not
 *                   found, fell through to JSON heuristic").
 */
public record Rendered(
        String strategy,
        String schemaRef,
        String text,
        int sizeBytes,
        boolean truncated,
        List<String> warnings) {

    public static Rendered tombstone() {
        return new Rendered("TOMBSTONE", null, "", 0, false, List.of());
    }

    public static Rendered empty() {
        return new Rendered("EMPTY", null, "", 0, false, List.of());
    }
}
