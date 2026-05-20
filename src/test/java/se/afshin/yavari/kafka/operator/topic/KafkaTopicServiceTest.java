package se.afshin.yavari.kafka.operator.topic;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicServiceTest {

    private final KafkaTopicService service = new KafkaTopicService();

    @Test
    void computeConfigDiff_addsMissingEntriesAsSet() {
        Map<String, String> current = Map.of();
        Map<String, String> desired = Map.of("retention.ms", "100");

        var ops = service.computeConfigDiff(current, desired);

        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.SET);
        assertThat(ops.get(0).configEntry().name()).isEqualTo("retention.ms");
        assertThat(ops.get(0).configEntry().value()).isEqualTo("100");
    }

    @Test
    void computeConfigDiff_changedValueIsSet() {
        var ops = service.computeConfigDiff(
                Map.of("retention.ms", "100"),
                Map.of("retention.ms", "200"));
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.SET);
        assertThat(ops.get(0).configEntry().value()).isEqualTo("200");
    }

    @Test
    void computeConfigDiff_unchangedEntryProducesNoOp() {
        var ops = service.computeConfigDiff(
                Map.of("retention.ms", "100"),
                Map.of("retention.ms", "100"));
        assertThat(ops).isEmpty();
    }

    @Test
    void computeConfigDiff_removedEntryBecomesDeleteOp() {
        var ops = service.computeConfigDiff(
                Map.of("retention.ms", "100", "cleanup.policy", "compact"),
                Map.of("cleanup.policy", "compact"));
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.DELETE);
        assertThat(ops.get(0).configEntry().name()).isEqualTo("retention.ms");
    }

    @Test
    void computeConfigDiff_mixedAddAndRemove() {
        var ops = KafkaTopicService.sorted(service.computeConfigDiff(
                Map.of("retention.ms", "100"),
                Map.of("cleanup.policy", "compact")));

        assertThat(ops).hasSize(2);
        assertThat(ops.get(0).configEntry().name()).isEqualTo("cleanup.policy");
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.SET);
        assertThat(ops.get(1).configEntry().name()).isEqualTo("retention.ms");
        assertThat(ops.get(1).opType()).isEqualTo(AlterConfigOp.OpType.DELETE);
    }

    @Test
    void computeConfigDiff_nullDesiredTreatedAsEmpty() {
        var ops = service.computeConfigDiff(Map.of("retention.ms", "100"), null);
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.DELETE);
    }

    @Test
    void computeConfigDiff_nullCurrentTreatedAsEmpty() {
        var ops = service.computeConfigDiff(null, Map.of("retention.ms", "100"));
        assertThat(ops).hasSize(1);
        assertThat(ops.get(0).opType()).isEqualTo(AlterConfigOp.OpType.SET);
    }

    @Test
    void computePartitionAction_equalIsNoOp() {
        assertThat(service.computePartitionAction(3, 3))
                .isEqualTo(KafkaTopicService.PartitionAction.NONE);
    }

    @Test
    void computePartitionAction_increaseIsExpand() {
        assertThat(service.computePartitionAction(3, 6))
                .isEqualTo(KafkaTopicService.PartitionAction.EXPAND);
    }

    @Test
    void computePartitionAction_decreaseIsRejected() {
        assertThat(service.computePartitionAction(6, 3))
                .isEqualTo(KafkaTopicService.PartitionAction.REJECT_DECREASE);
    }
}
