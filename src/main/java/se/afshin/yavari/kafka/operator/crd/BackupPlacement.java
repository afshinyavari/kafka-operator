package se.afshin.yavari.kafka.operator.crd;

/**
 * Pins a backup/restore/validation workload to exactly one Kubernetes cluster.
 *
 * <p>One {@link KafkaCluster} spans every MCS member cluster, and the same CR is
 * applied to all of them — so without a placement gate each of the N operator
 * instances would build its own CronJob/Job and the backup would run N times.
 * {@code clusterId} must match the operator's {@code kafka.cluster.id}; non-matching
 * instances skip the CR ({@code status.phase=SKIPPED}). It is required whenever the
 * operator runs multi-cluster — see {@code BackupPlacementGate}.
 */
public class BackupPlacement {

    private String clusterId;

    public String getClusterId() { return clusterId; }
    public void setClusterId(String clusterId) { this.clusterId = clusterId; }
}
