package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/**
 * Result of a bounded server-side message scan. {@code scanned} is how many
 * records were examined; {@code truncated} is true if the scan cap was hit
 * before reaching the end of the partition.
 */
public record SearchResult(
        int scanned,
        boolean truncated,
        List<RenderedRecord> matches) {
}
