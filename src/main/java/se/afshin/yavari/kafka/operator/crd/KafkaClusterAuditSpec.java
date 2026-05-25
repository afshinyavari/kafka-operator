package se.afshin.yavari.kafka.operator.crd;

import java.util.Set;

/**
 * Unified audit logging for the cluster. Optional sub-spec on
 * {@link KafkaClusterSpec}.
 *
 * <p>The in-process audit emitter is always active in both Kroxylicious and the
 * Apicurio rbac-proxy — every authorisation decision is logged as one JSON line
 * on the dedicated {@code kafka-audit} SLF4J channel. This sub-spec controls
 * the <i>opt-in</i> Kafka-topic sink and lets operators trim cardinality with
 * an op allowlist.
 *
 * <p>Set {@code spec.audit: {}} to keep the defaults (stdout-only, all ops).
 */
public class KafkaClusterAuditSpec {

    /** Kafka-topic sink for the audit stream. Null = stdout only. */
    private AuditKafkaTopicSpec kafkaTopic;

    /**
     * Optional allowlist of operation names to emit ({@code PRODUCE},
     * {@code FETCH}, {@code CREATE_TOPICS}, ...). When null or empty, every
     * decision is emitted. Use this to drop the busiest verbs (typically
     * {@code FETCH}) so the {@code __audit} topic stays useful.
     */
    private Set<String> includeOps;

    public AuditKafkaTopicSpec getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(AuditKafkaTopicSpec kafkaTopic) { this.kafkaTopic = kafkaTopic; }

    public Set<String> getIncludeOps() { return includeOps; }
    public void setIncludeOps(Set<String> includeOps) { this.includeOps = includeOps; }
}
