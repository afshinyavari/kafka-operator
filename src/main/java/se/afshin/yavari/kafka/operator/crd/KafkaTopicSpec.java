package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.HashMap;
import java.util.Map;

public class KafkaTopicSpec {

    /** Name of the KafkaCluster CR in the same namespace this topic belongs to. */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "clusterRef must not be blank")
    private String clusterRef;

    /** Optional override for the Kafka topic name. When unset, metadata.name is used.
     *  Use this only when metadata.name (lowercase [a-z0-9.-]) cannot express the desired
     *  topic name (e.g. uppercase or underscore characters). */
    @ValidationRule(
        value = "self.matches('^[a-zA-Z0-9._-]{1,249}$')",
        message = "topicName must match Kafka's valid topic name pattern (1-249 chars, [a-zA-Z0-9._-])"
    )
    private String topicName;

    @ValidationRule(value = "self >= 1", message = "partitions must be at least 1")
    private int partitions = 1;

    @ValidationRule(value = "self >= 1", message = "replicationFactor must be at least 1")
    private short replicationFactor = 1;

    /** Dynamic topic-level configs (e.g. cleanup.policy, retention.ms).
     *  Keys not listed here are reset to broker defaults on each reconcile. */
    private Map<String, String> config = new HashMap<>();

    /** What to do with the Kafka topic when the KafkaTopic CR is deleted. */
    private TopicDeletionPolicy deletionPolicy = TopicDeletionPolicy.DELETE;

    public String getClusterRef() { return clusterRef; }
    public void setClusterRef(String clusterRef) { this.clusterRef = clusterRef; }

    public String getTopicName() { return topicName; }
    public void setTopicName(String topicName) { this.topicName = topicName; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }

    public TopicDeletionPolicy getDeletionPolicy() { return deletionPolicy; }
    public void setDeletionPolicy(TopicDeletionPolicy deletionPolicy) { this.deletionPolicy = deletionPolicy; }
}
