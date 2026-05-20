package se.afshin.yavari.kafka.operator.topic;

import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;

/**
 * Decides which operator instance (in MCS topology) is the single writer for a
 * KafkaTopic's reconcile work. Other instances mark the CR SKIPPED.
 * v1 impl is {@link StaticPrimaryClusterLeader}. v2 will swap in a Kafka
 * consumer-group-based leader implementation without callsite changes.
 */
public interface TopicReconcileLeader {

    /** @return true if {@code localClusterId} is the elected leader for this topic. */
    boolean isLeader(KafkaTopic topic, KafkaCluster cluster, String localClusterId);

    /** Human-readable id of the current leader, used for SKIPPED status messages. */
    String currentLeaderId(KafkaCluster cluster);
}
