package se.afshin.yavari.kafka.operator.rolling;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IsrCheckerTest {

    /**
     * When Kafka is unreachable (no server running), IsrChecker must return true
     * (treat as safe) so the operator doesn't get stuck on a fresh install.
     */
    @Test
    void brokerUnreachableIsTreatedAsSafe() {
        IsrChecker checker = new IsrChecker();
        boolean safe = checker.isBrokerSafeToRestart("localhost:19092", 999);
        assertThat(safe).isTrue();
    }

    @Test
    void controllerUnreachableIsTreatedAsSafe() {
        IsrChecker checker = new IsrChecker();
        boolean safe = checker.isControllerSafeToRestart("localhost:19093", 1999);
        assertThat(safe).isTrue();
    }
}
