package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupTopicSelector;
import se.afshin.yavari.kafka.operator.crd.KafkaRestore;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreStatus;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRestoreReconcilerTest {

    @Test
    void terminalPhasesBlockReRun() {
        assertThat(KafkaRestoreReconciler.isTerminal(KafkaRestoreStatus.Phase.SUCCEEDED)).isTrue();
        assertThat(KafkaRestoreReconciler.isTerminal(KafkaRestoreStatus.Phase.FAILED)).isTrue();
        assertThat(KafkaRestoreReconciler.isTerminal(KafkaRestoreStatus.Phase.SKIPPED)).isTrue();
    }

    @Test
    void nonTerminalPhasesAllowReconcile() {
        assertThat(KafkaRestoreReconciler.isTerminal(KafkaRestoreStatus.Phase.PENDING)).isFalse();
        assertThat(KafkaRestoreReconciler.isTerminal(KafkaRestoreStatus.Phase.RUNNING)).isFalse();
    }

    @Test
    void literalTargetTopicsSkipsGlobsAndAppliesMapping() {
        KafkaRestoreSpec spec = new KafkaRestoreSpec();
        BackupTopicSelector topics = new BackupTopicSelector();
        topics.setInclude(List.of("orders", "events-*", "payments"));
        spec.setTopics(topics);
        spec.setTopicMapping(Map.of("orders", "orders-restored"));

        assertThat(KafkaRestoreReconciler.literalTargetTopics(spec))
                .containsExactlyInAnyOrder("orders-restored", "payments");
    }

    @Test
    void literalTargetTopicsEmptyWhenAllGlobs() {
        KafkaRestoreSpec spec = new KafkaRestoreSpec();
        BackupTopicSelector topics = new BackupTopicSelector();
        topics.setInclude(List.of("*"));
        spec.setTopics(topics);
        assertThat(KafkaRestoreReconciler.literalTargetTopics(spec)).isEmpty();
    }

    @Test
    void ownerRefIsAControllerReference() {
        KafkaRestore cr = new KafkaRestore();
        cr.setMetadata(new ObjectMetaBuilder()
                .withName("restore-1").withNamespace("kafka").withUid("uid-9").build());
        var ref = KafkaRestoreReconciler.ownerRef(cr);
        assertThat(ref.getName()).isEqualTo("restore-1");
        assertThat(ref.getController()).isTrue();
    }
}
