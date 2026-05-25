package se.afshin.yavari.kafka.editor.admin.dto;

/** Where a produced (or replayed) record landed. */
public record SendResult(String topic, int partition, long offset, long timestamp) {
}
