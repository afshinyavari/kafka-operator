package se.afshin.yavari.kafka.operator.backup;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidationStatus;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaBackupValidationReconcilerTest {

    @Test
    void terminalPhasesBlockReRun() {
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.VALID)).isTrue();
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.INVALID)).isTrue();
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.FAILED)).isTrue();
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.SKIPPED)).isTrue();
    }

    @Test
    void nonTerminalPhasesAllowReconcile() {
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.PENDING)).isFalse();
        assertThat(KafkaBackupValidationReconciler.isTerminal(
                KafkaBackupValidationStatus.Phase.RUNNING)).isFalse();
    }
}
