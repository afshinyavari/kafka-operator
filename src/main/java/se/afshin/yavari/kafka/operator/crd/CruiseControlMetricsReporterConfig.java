package se.afshin.yavari.kafka.operator.crd;

/**
 * Optional tuning for the broker-side Cruise Control Metrics Reporter. The reporter runs
 * inside every broker JVM and publishes broker/partition metrics to the
 * {@code __CruiseControlMetrics} topic. Both fields are nullable — null leaves the reporter
 * at its own defaults (the operator still derives a safe replication factor when unset).
 */
public class CruiseControlMetricsReporterConfig {

    /** Replication factor for the {@code __CruiseControlMetrics} topic. Null → the operator
     *  uses {@code min(3, brokerCount)}, matching its internal-topic RF policy. */
    private Integer metricsTopicReplicas;

    /** Partition count for the {@code __CruiseControlMetrics} topic. Null → reporter default. */
    private Integer metricsTopicPartitions;

    public Integer getMetricsTopicReplicas() { return metricsTopicReplicas; }
    public void setMetricsTopicReplicas(Integer metricsTopicReplicas) {
        this.metricsTopicReplicas = metricsTopicReplicas;
    }

    public Integer getMetricsTopicPartitions() { return metricsTopicPartitions; }
    public void setMetricsTopicPartitions(Integer metricsTopicPartitions) {
        this.metricsTopicPartitions = metricsTopicPartitions;
    }
}
