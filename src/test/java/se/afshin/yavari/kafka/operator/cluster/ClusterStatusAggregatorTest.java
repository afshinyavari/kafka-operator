package se.afshin.yavari.kafka.operator.cluster;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterStatusAggregatorTest {

    private final ClusterStatusAggregator aggregator = new ClusterStatusAggregator();

    @Test
    void allPodsReady_returnsReadyPhase() {
        KafkaClusterStatus status = new KafkaClusterStatus();
        KafkaPodSet ps = podSet("broker-a", 3, 3);

        aggregator.aggregate(status, List.of(ps));

        assertThat(status.getPhase()).isEqualTo(KafkaClusterStatus.Phase.READY);
        assertThat(status.getMessage()).contains("3");
    }

    @Test
    void somePodsNotReady_returnsReconcilingPhase() {
        KafkaClusterStatus status = new KafkaClusterStatus();
        KafkaPodSet ps = podSet("broker-a", 1, 3);

        aggregator.aggregate(status, List.of(ps));

        assertThat(status.getPhase()).isEqualTo(KafkaClusterStatus.Phase.RECONCILING);
        assertThat(status.getMessage()).contains("1/3");
    }

    @Test
    void noPodSets_returnsReconcilingWithNoPoolsMessage() {
        KafkaClusterStatus status = new KafkaClusterStatus();

        aggregator.aggregate(status, List.of());

        assertThat(status.getPhase()).isEqualTo(KafkaClusterStatus.Phase.RECONCILING);
        assertThat(status.getMessage()).contains("No KafkaNodePools");
    }

    @Test
    void multiplePools_sumsReplicasAndPopulatesPoolPhases() {
        KafkaClusterStatus status = new KafkaClusterStatus();
        KafkaPodSet brokers = podSet("brokers", 3, 3);
        KafkaPodSet controllers = podSet("controllers", 1, 3);

        aggregator.aggregate(status, List.of(brokers, controllers));

        assertThat(status.getPhase()).isEqualTo(KafkaClusterStatus.Phase.RECONCILING);
        assertThat(status.getPoolPhases()).containsEntry("brokers", "3/3");
        assertThat(status.getPoolPhases()).containsEntry("controllers", "1/3");
    }

    @Test
    void podSetWithNullStatus_isSkippedInAggregation() {
        KafkaClusterStatus status = new KafkaClusterStatus();
        KafkaPodSet withStatus = podSet("pool-a", 2, 2);
        KafkaPodSet noStatus = podSetNoStatus("pool-b");

        aggregator.aggregate(status, List.of(withStatus, noStatus));

        assertThat(status.getPhase()).isEqualTo(KafkaClusterStatus.Phase.READY);
        assertThat(status.getPoolPhases()).containsKey("pool-a");
        assertThat(status.getPoolPhases()).doesNotContainKey("pool-b");
    }

    // --- Helpers ---

    private KafkaPodSet podSet(String name, int readyReplicas, int replicas) {
        KafkaPodSet ps = new KafkaPodSet();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        ps.setMetadata(meta);

        KafkaPodSetStatus s = new KafkaPodSetStatus();
        s.setReadyReplicas(readyReplicas);
        s.setReplicas(replicas);
        ps.setStatus(s);
        return ps;
    }

    private KafkaPodSet podSetNoStatus(String name) {
        KafkaPodSet ps = new KafkaPodSet();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        ps.setMetadata(meta);
        // status intentionally left null
        return ps;
    }
}
