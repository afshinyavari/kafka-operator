package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.crd.Mm2FlowConfig;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Mm2InternalTopicsBuilderTest {

    private final Mm2InternalTopicsBuilder builder = new Mm2InternalTopicsBuilder();

    private MirrorMaker2 cr(String flowName, int rf) {
        MirrorMaker2 cr = new MirrorMaker2();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-mm2").withNamespace("kafka").build());
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        Mm2FlowConfig f = new Mm2FlowConfig();
        f.setFlowName(flowName);
        f.setReplicationFactor(rf);
        spec.setFlow(f);
        cr.setSpec(spec);
        return cr;
    }

    private OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withName("my-mm2").withUid("uid").withKind("MirrorMaker2")
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withController(true).withBlockOwnerDeletion(true).build();
    }

    @Test
    void threeTopicsWithFlowSuffixAndOwnerRef() {
        List<KafkaTopic> topics = builder.build(cr(null, 3), "dr", ownerRef());

        assertThat(topics).hasSize(3);
        assertThat(topics).extracting(t -> t.getMetadata().getName())
                .containsExactly("mm2-configs.my-mm2", "mm2-offsets.my-mm2", "mm2-status.my-mm2");
        for (KafkaTopic t : topics) {
            assertThat(t.getSpec().getClusterRef()).isEqualTo("dr");
            assertThat(t.getSpec().getReplicationFactor()).isEqualTo(3);
            assertThat(t.getSpec().getConfig()).containsEntry("cleanup.policy", "compact");
            assertThat(t.getMetadata().getOwnerReferences()).hasSize(1);
            assertThat(t.getSpec().getDeletionPolicy()).isEqualTo(TopicDeletionPolicy.DELETE);
        }
    }

    @Test
    void partitionsMatchMm2Convention() {
        List<KafkaTopic> topics = builder.build(cr("flow", 3), "dr", ownerRef());

        // mm2-configs = 1 partition (Kafka Connect config storage convention)
        // mm2-offsets = 25 partitions
        // mm2-status = 5 partitions
        assertThat(topics.get(0).getSpec().getPartitions()).isEqualTo(1);
        assertThat(topics.get(1).getSpec().getPartitions()).isEqualTo(25);
        assertThat(topics.get(2).getSpec().getPartitions()).isEqualTo(5);
    }

    @Test
    void minIsrFollowsRf() {
        List<KafkaTopic> topics = builder.build(cr("flow", 3), "dr", ownerRef());
        for (KafkaTopic t : topics) {
            assertThat(t.getSpec().getConfig()).containsEntry("min.insync.replicas", "2");
        }
        List<KafkaTopic> rf1 = builder.build(cr("flow", 1), "dr", ownerRef());
        for (KafkaTopic t : rf1) {
            assertThat(t.getSpec().getConfig()).containsEntry("min.insync.replicas", "1");
        }
    }
}
