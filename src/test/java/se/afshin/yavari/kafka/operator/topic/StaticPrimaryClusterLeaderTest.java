package se.afshin.yavari.kafka.operator.topic;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StaticPrimaryClusterLeaderTest {

    private final StaticPrimaryClusterLeader leader = new StaticPrimaryClusterLeader();

    @Test
    void firstClusterIsLeader() {
        KafkaCluster cluster = cluster("a", "b", "c");
        assertThat(leader.isLeader(new KafkaTopic(), cluster, "a")).isTrue();
        assertThat(leader.isLeader(new KafkaTopic(), cluster, "b")).isFalse();
        assertThat(leader.isLeader(new KafkaTopic(), cluster, "c")).isFalse();
    }

    @Test
    void emptyClustersList_isLeaderReturnsFalse() {
        KafkaCluster cluster = cluster();
        assertThat(leader.isLeader(new KafkaTopic(), cluster, "a")).isFalse();
        assertThat(leader.currentLeaderId(cluster)).isNull();
    }

    @Test
    void nullSpec_isLeaderReturnsFalse() {
        KafkaCluster cluster = new KafkaCluster();
        assertThat(leader.isLeader(new KafkaTopic(), cluster, "a")).isFalse();
        assertThat(leader.currentLeaderId(cluster)).isNull();
    }

    @Test
    void currentLeaderId_returnsFirstClusterId() {
        assertThat(leader.currentLeaderId(cluster("alpha", "beta"))).isEqualTo("alpha");
    }

    private KafkaCluster cluster(String... ids) {
        KafkaCluster c = new KafkaCluster();
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(java.util.Arrays.stream(ids).map(id -> {
            ClusterEntry e = new ClusterEntry();
            e.setId(id);
            return e;
        }).toList());
        c.setSpec(spec);
        return c;
    }
}
