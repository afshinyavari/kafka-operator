package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicSpec;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.Mm2FlowConfig;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.util.List;
import java.util.Map;

/**
 * Generates the three MM2-internal {@link KafkaTopic} CRs on the target managed
 * KafkaCluster. The CRs are owner-ref'd to the MM2 CR so they cascade on deletion.
 *
 * <p>When the target end is external (no managed KafkaCluster), the operator skips
 * topic creation and lets the worker auto-create on first start (or fail loudly if
 * the external broker forbids that).
 */
@ApplicationScoped
public class Mm2InternalTopicsBuilder {

    public List<KafkaTopic> build(MirrorMaker2 cr, String targetClusterName, OwnerReference ownerRef) {
        Mm2FlowConfig flow = cr.getSpec().getFlow();
        int rf = flow != null ? flow.getReplicationFactor() : 3;
        String flowName = cr.resolvedFlowName();
        String namespace = cr.getMetadata().getNamespace();
        return List.of(
                topic("mm2-configs." + flowName, namespace, targetClusterName, ownerRef, 1, rf),
                topic("mm2-offsets." + flowName, namespace, targetClusterName, ownerRef, 25, rf),
                topic("mm2-status." + flowName, namespace, targetClusterName, ownerRef, 5, rf));
    }

    private KafkaTopic topic(String name, String namespace, String clusterRef,
                              OwnerReference ownerRef, int partitions, int rf) {
        KafkaTopic t = new KafkaTopic();
        t.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(Mm2Labels.labels(clusterRef))
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
