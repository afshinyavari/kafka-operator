package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectWorkerConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectInternalTopicsBuilderTest {

    private final ConnectInternalTopicsBuilder builder = new ConnectInternalTopicsBuilder();

    private KafkaConnect cr(int rf) {
        KafkaConnect cr = new KafkaConnect();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-connect").withNamespace("kafka").build());
        KafkaConnectSpec spec = new KafkaConnectSpec();
        KafkaConnectWorkerConfig w = new KafkaConnectWorkerConfig();
        w.setInternalReplicationFactor(rf);
        spec.setWorker(w);
        cr.setSpec(spec);
        return cr;
    }

    private OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withName("my-connect").withUid("uid").withKind("KafkaConnect")
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withController(true).withBlockOwnerDeletion(true).build();
    }

    @Test
    void threeTopicsSuffixedByCrNameWithOwnerRef() {
        List<KafkaTopic> topics = builder.build(cr(3), "my-kafka", ownerRef());

        assertThat(topics).hasSize(3);
        assertThat(topics).extracting(t -> t.getMetadata().getName())
                .containsExactly("connect-configs.my-connect",
                                 "connect-offsets.my-connect",
                                 "connect-status.my-connect");
        for (KafkaTopic t : topics) {
            assertThat(t.getSpec().getClusterRef()).isEqualTo("my-kafka");
            assertThat(t.getSpec().getReplicationFactor()).isEqualTo(3);
            assertThat(t.getSpec().getConfig()).containsEntry("cleanup.policy", "compact");
            assertThat(t.getMetadata().getOwnerReferences()).hasSize(1);
            assertThat(t.getSpec().getDeletionPolicy()).isEqualTo(TopicDeletionPolicy.DELETE);
        }
    }

    @Test
    void partitionsMatchConnectConvention() {
        List<KafkaTopic> topics = builder.build(cr(3), "my-kafka", ownerRef());

        assertThat(topics.get(0).getSpec().getPartitions()).isEqualTo(1);
        assertThat(topics.get(1).getSpec().getPartitions()).isEqualTo(25);
        assertThat(topics.get(2).getSpec().getPartitions()).isEqualTo(5);
    }

    @Test
    void minIsrFollowsRf() {
        for (KafkaTopic t : builder.build(cr(3), "my-kafka", ownerRef())) {
            assertThat(t.getSpec().getConfig()).containsEntry("min.insync.replicas", "2");
        }
        for (KafkaTopic t : builder.build(cr(1), "my-kafka", ownerRef())) {
            assertThat(t.getSpec().getConfig()).containsEntry("min.insync.replicas", "1");
        }
    }
}
