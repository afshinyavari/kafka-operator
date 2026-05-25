package se.afshin.yavari.kafka.editor.admin.dto;

/**
 * Consumer lag for one (topic, partition). {@code committedOffset} and
 * {@code lag} are null when the group has no committed offset there.
 */
public record PartitionLag(
        String topic,
        int partition,
        Long committedOffset,
        long endOffset,
        Long lag) {
}
