package se.afshin.yavari.kafka.operator.topic;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;

@ApplicationScoped
public class StaticPrimaryClusterLeader implements TopicReconcileLeader {

    @Override
    public boolean isLeader(KafkaTopic topic, KafkaCluster cluster, String localClusterId) {
        String leader = currentLeaderId(cluster);
        return leader != null && leader.equals(localClusterId);
    }

    @Override
    public String currentLeaderId(KafkaCluster cluster) {
        if (cluster == null || cluster.getSpec() == null
                || cluster.getSpec().getClusters() == null
                || cluster.getSpec().getClusters().isEmpty()) {
            return null;
        }
        return cluster.getSpec().getClusters().get(0).getId();
    }
}
