package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

/**
 * Optional Kafka-topic sink for audit events. When {@code enabled=true}, the
 * operator upserts a {@link KafkaTopic} for the audit stream on the parent
 * cluster and injects {@code KAFKA_AUDIT_*} env on the proxy and Apicurio
 * rbac-proxy Deployments so the in-process emitter ships to it.
 *
 * <p>Audit traffic goes <b>direct to the broker INTERNAL listener</b> (using
 * the proxy's existing mTLS client cert), never via Kroxylicious — avoiding any
 * "audit-the-audit" loop.
 */
public class AuditKafkaTopicSpec {

    private boolean enabled = false;

    /** Topic name. Defaults to {@code __audit}. */
    @ValidationRule(
        value = "self.matches('^[a-zA-Z0-9._-]{1,249}$')",
        message = "name must match Kafka's valid topic name pattern (1-249 chars, [a-zA-Z0-9._-])"
    )
    private String name = "__audit";

    @ValidationRule(value = "self >= 1 && self <= 365",
            message = "retentionDays must be between 1 and 365")
    private int retentionDays = 30;

    @ValidationRule(value = "self >= 1", message = "partitions must be at least 1")
    private int partitions = 3;

    @ValidationRule(value = "self >= 1", message = "replicationFactor must be at least 1")
    private int replicationFactor = 3;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getRetentionDays() { return retentionDays; }
    public void setRetentionDays(int retentionDays) { this.retentionDays = retentionDays; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }
}
