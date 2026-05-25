package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.List;

/** Cluster dashboard summary. {@code controller} may be null during election. */
public record ClusterOverview(
        String clusterId,
        BrokerInfo controller,
        List<BrokerInfo> brokers,
        int topicCount,
        int partitionCount) {
}
