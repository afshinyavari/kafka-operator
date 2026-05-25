package se.afshin.yavari.kafka.editor.admin.dto;

/** Offset span of one partition. {@code count = endOffset - startOffset}. */
public record PartitionMetric(
        int partition,
        long startOffset,
        long endOffset,
        long count) {
}
