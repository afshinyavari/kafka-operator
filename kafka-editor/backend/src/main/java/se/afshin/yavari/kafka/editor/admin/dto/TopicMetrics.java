package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/**
 * Aggregate health/size metrics for one topic. {@code sizeBytes} is -1 when
 * the broker did not report log-dir sizes. {@code messageCount} is the offset
 * sum and is approximate on compacted topics.
 */
public record TopicMetrics(
        int partitionCount,
        long messageCount,
        long sizeBytes,
        int underReplicatedPartitions,
        int offlinePartitions,
        List<PartitionMetric> partitions) {
}
