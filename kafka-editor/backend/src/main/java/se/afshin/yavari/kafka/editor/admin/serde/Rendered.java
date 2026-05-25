package se.afshin.yavari.kafka.editor.admin.serde;

import java.util.List;

/**
 * A decoded view of one record's key or value. {@code strategy} is how it was
 * rendered: TOMBSTONE, EMPTY, AVRO, JSON, STRING or BINARY.
 */
public record Rendered(
        String strategy,
        String schemaRef,
        String text,
        int size,
        boolean truncated,
        List<String> warnings) {

    public static Rendered tombstone() {
        return new Rendered("TOMBSTONE", null, null, 0, false, List.of());
    }

    public static Rendered empty() {
        return new Rendered("EMPTY", null, "", 0, false, List.of());
    }
}
