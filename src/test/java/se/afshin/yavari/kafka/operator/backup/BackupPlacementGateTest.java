package se.afshin.yavari.kafka.operator.backup;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupPlacement;

import static org.assertj.core.api.Assertions.assertThat;

class BackupPlacementGateTest {

    private BackupPlacementGate gate(String localClusterId) {
        BackupPlacementGate g = new BackupPlacementGate();
        g.localClusterId = localClusterId;
        return g;
    }

    private BackupPlacement placement(String clusterId) {
        BackupPlacement p = new BackupPlacement();
        p.setClusterId(clusterId);
        return p;
    }

    @Test
    void runsWhenPlacementMatchesLocalCluster() {
        assertThat(gate("kafka-a").evaluate(placement("kafka-a")))
                .isEqualTo(BackupPlacementGate.Decision.RUN);
    }

    @Test
    void skipsWhenPlacementIsAnotherCluster() {
        assertThat(gate("kafka-a").evaluate(placement("kafka-b")))
                .isEqualTo(BackupPlacementGate.Decision.SKIP);
    }

    @Test
    void rejectsMissingPlacementOnMultiClusterOperator() {
        assertThat(gate("kafka-a").evaluate(null))
                .isEqualTo(BackupPlacementGate.Decision.MISSING_PLACEMENT);
    }

    @Test
    void runsWithoutPlacementOnSingleClusterOperator() {
        assertThat(gate("").evaluate(null)).isEqualTo(BackupPlacementGate.Decision.RUN);
    }

    @Test
    void runsWithPlacementOnSingleClusterOperator() {
        assertThat(gate("").evaluate(placement("kafka-a")))
                .isEqualTo(BackupPlacementGate.Decision.RUN);
    }
}
