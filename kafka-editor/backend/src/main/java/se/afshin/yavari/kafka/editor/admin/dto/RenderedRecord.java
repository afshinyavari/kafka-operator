package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

import se.afshin.yavari.kafka.editor.admin.serde.Rendered;

/** One consumed record, key and value smart-decoded. */
public record RenderedRecord(
        int partition,
        long offset,
        long timestamp,
        Rendered key,
        Rendered value,
        List<HeaderKv> headers) {
}
