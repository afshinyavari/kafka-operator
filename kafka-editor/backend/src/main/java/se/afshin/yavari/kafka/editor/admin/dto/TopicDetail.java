package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/** Full detail for one topic: partitions and (best-effort) configuration. */
public record TopicDetail(
        String name,
        boolean internal,
        List<PartitionDetail> partitions,
        List<ConfigKv> configs) {
}
