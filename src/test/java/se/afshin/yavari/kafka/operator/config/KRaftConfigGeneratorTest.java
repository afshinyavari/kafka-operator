package se.afshin.yavari.kafka.operator.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KRaftConfigGeneratorTest {

    private KRaftConfigGenerator gen;

    @BeforeEach
    void setup() {
        gen = new KRaftConfigGenerator();
    }

    @Test
    void controllerNodeId() {
        assertThat(gen.controllerNodeId(0)).isEqualTo(10000);
        assertThat(gen.controllerNodeId(1)).isEqualTo(10001);
        assertThat(gen.controllerNodeId(2)).isEqualTo(10002);
    }

    @Test
    void brokerNodeId() {
        assertThat(gen.brokerNodeId(0, 0)).isEqualTo(0);
        assertThat(gen.brokerNodeId(0, 2)).isEqualTo(2);
        assertThat(gen.brokerNodeId(1, 0)).isEqualTo(1000);
        assertThat(gen.brokerNodeId(2, 1)).isEqualTo(2001);
    }

    @Test
    void quorumVotersForThreeClusters() {
        KafkaClusterSpec spec = specWith(
                cluster("A", "ctrl-a.example.com:9093"),
                cluster("B", "ctrl-b.example.com:9093"),
                cluster("C", "ctrl-c.example.com:9093")
        );
        assertThat(gen.buildQuorumVoters(spec))
                .isEqualTo("10000@ctrl-a.example.com:9093,10001@ctrl-b.example.com:9093,10002@ctrl-c.example.com:9093");
    }

    @Test
    void quorumVotersThrowsOnBlankAddress() {
        KafkaClusterSpec spec = specWith(cluster("A", ""));
        assertThatThrownBy(() -> gen.buildQuorumVoters(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("controllerAdvertisedAddress is required");
    }

    @Test
    void quorumVotersThrowsOnEmptyClusters() {
        KafkaClusterSpec spec = new KafkaClusterSpec();
        assertThatThrownBy(() -> gen.buildQuorumVoters(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void clusterIndexFound() {
        KafkaClusterSpec spec = specWith(cluster("A", "x:9093"), cluster("B", "y:9093"), cluster("C", "z:9093"));
        assertThat(gen.clusterIndex(spec, "A")).isEqualTo(0);
        assertThat(gen.clusterIndex(spec, "B")).isEqualTo(1);
        assertThat(gen.clusterIndex(spec, "C")).isEqualTo(2);
    }

    @Test
    void clusterIndexThrowsWhenNotFound() {
        KafkaClusterSpec spec = specWith(cluster("A", "x:9093"));
        assertThatThrownBy(() -> gen.clusterIndex(spec, "Z"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not found in spec.clusters");
    }

    @Test
    void clusterIdFromNameAndNamespace() {
        KafkaCluster cr = new KafkaCluster();
        cr.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMeta());
        cr.getMetadata().setNamespace("kafka");
        cr.getMetadata().setName("my-kafka");
        String id = gen.clusterIdFrom(cr);
        assertThat(id).hasSize(22);
        // Must be valid base64url chars only
        assertThat(id).matches("[A-Za-z0-9_-]{22}");
        // Must be deterministic — same input always gives same output
        assertThat(gen.clusterIdFrom(cr)).isEqualTo(id);
    }

    // Helpers

    private static KafkaClusterSpec specWith(ClusterEntry... entries) {
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(List.of(entries));
        return spec;
    }

    private static ClusterEntry cluster(String id, String addr) {
        ClusterEntry e = new ClusterEntry();
        e.setId(id);
        e.setControllerAdvertisedAddress(addr);
        return e;
    }
}
