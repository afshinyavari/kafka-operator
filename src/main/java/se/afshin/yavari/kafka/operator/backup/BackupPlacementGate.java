package se.afshin.yavari.kafka.operator.backup;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.crd.BackupPlacement;

/**
 * Decides whether the local operator instance should act on a backup CR.
 *
 * <p>One {@code KafkaCluster} spans every MCS member cluster and the same backup CR is
 * applied to all of them. Without a gate each of the N operator instances would build
 * its own CronJob/Job and the backup would run N times. {@code spec.placement.clusterId}
 * pins the work to exactly one cluster.
 */
@ApplicationScoped
public class BackupPlacementGate {

    public enum Decision {
        /** This instance owns the CR — reconcile it. */
        RUN,
        /** Another cluster owns the CR — skip. */
        SKIP,
        /** The operator is multi-cluster but the CR has no placement — reject. */
        MISSING_PLACEMENT
    }

    @ConfigProperty(name = "kafka.cluster.id", defaultValue = "")
    String localClusterId;

    public Decision evaluate(BackupPlacement placement) {
        String configured = placement != null ? placement.getClusterId() : null;
        boolean haveConfigured = configured != null && !configured.isBlank();
        boolean multiCluster = localClusterId != null && !localClusterId.isBlank();

        if (!haveConfigured) {
            // Single-cluster operators may omit placement; multi-cluster must not.
            return multiCluster ? Decision.MISSING_PLACEMENT : Decision.RUN;
        }
        if (!multiCluster) {
            return Decision.RUN;
        }
        return configured.equals(localClusterId) ? Decision.RUN : Decision.SKIP;
    }

    public String localClusterId() { return localClusterId; }
}
