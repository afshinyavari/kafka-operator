package se.afshin.yavari.kafka.operator.cluster;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.util.List;

@ApplicationScoped
public class ClusterStatusAggregator {

    public void aggregate(KafkaClusterStatus status, List<KafkaPodSet> podSets) {
        int totalReady = 0;
        int totalDesired = 0;

        for (KafkaPodSet ps : podSets) {
            if (ps.getStatus() != null) {
                totalReady += ps.getStatus().getReadyReplicas();
                totalDesired += ps.getStatus().getReplicas();
                status.getPoolPhases().put(ps.getMetadata().getName(),
                        ps.getStatus().getReadyReplicas() + "/" + ps.getStatus().getReplicas());
            }
        }

        if (totalReady < totalDesired) {
            status.setPhase(KafkaClusterStatus.Phase.RECONCILING);
            status.setMessage("Waiting for pods: " + totalReady + "/" + totalDesired + " ready");
        } else if (totalDesired == 0) {
            status.setPhase(KafkaClusterStatus.Phase.RECONCILING);
            status.setMessage("No KafkaNodePools found yet — apply KafkaNodePool resources");
        } else {
            status.setPhase(KafkaClusterStatus.Phase.READY);
            status.setMessage("All " + totalReady + " pods ready");
        }
    }
}
