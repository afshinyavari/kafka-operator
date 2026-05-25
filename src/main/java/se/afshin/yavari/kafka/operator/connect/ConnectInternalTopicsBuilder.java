package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicSpec;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.util.List;
import java.util.Map;

/**
 * Generates the three Connect-internal {@link KafkaTopic} CRs on the attached managed
 * KafkaCluster. The CRs are owner-ref'd to the KafkaConnect CR so they cascade on
 * deletion. Skipped when the attachment is external (Connect will then auto-create on
 * first start, or fail loudly if the external broker forbids that).
 */
@ApplicationScoped
public class ConnectInternalTopicsBuilder {

    public List<KafkaTopic> build(KafkaConnect cr, String targetClusterName, OwnerReference ownerRef) {
        int rf = cr.getSpec().getWorker() != null
                ? cr.getSpec().getWorker().getInternalReplicationFactor() : 3;
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        return List.of(
                topic("connect-configs." + name, namespace, targetClusterName, ownerRef, 1, rf),
                topic("connect-offsets." + name, namespace, targetClusterName, ownerRef, 25, rf),
                topic("connect-status." + name, namespace, targetClusterName, ownerRef, 5, rf));
    }

    private KafkaTopic topic(String topicName, String namespace, String clusterRef,
                              OwnerReference ownerRef, int partitions, int rf) {
        KafkaTopic t = new KafkaTopic();
        t.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName(topicName)
                .withNamespace(namespace)
                .withLabels(ConnectLabels.labels(clusterRef))
                .withOwnerReferences(ownerRef)
                .build());
        KafkaTopicSpec spec = new KafkaTopicSpec();
        spec.setClusterRef(clusterRef);
        spec.setPartitions(partitions);
        spec.setReplicationFactor(rf);
        spec.setConfig(Map.of(
                "cleanup.policy", "compact",
                "min.insync.replicas", String.valueOf(Math.max(1, rf - 1))));
        spec.setDeletionPolicy(TopicDeletionPolicy.DELETE);
        t.setSpec(spec);
        return t;
    }
}
