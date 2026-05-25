package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/** A consumer group's committed offsets and per-partition lag. */
public record GroupOffsets(
        String groupId,
        String state,
        long totalLag,
        List<PartitionLag> partitions) {
}
