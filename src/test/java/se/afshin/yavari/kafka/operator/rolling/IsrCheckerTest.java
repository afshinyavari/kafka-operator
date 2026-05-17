package se.afshin.yavari.kafka.operator.rolling;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.metrics.OperatorMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class IsrCheckerTest {

    private IsrChecker checker;

    @BeforeEach
    void setup() throws Exception {
        checker = new IsrChecker();
        var field = IsrChecker.class.getDeclaredField("metrics");
        field.setAccessible(true);
        field.set(checker, mock(OperatorMetrics.class));
    }

    /**
     * When Kafka is unreachable (no server running), IsrChecker must return true
     * (treat as safe) so the operator doesn't get stuck on a fresh install.
     */
    @Test
    void brokerUnreachableIsTreatedAsSafe() {
        boolean safe = checker.isBrokerSafeToRestart("localhost:19092", 999);
        assertThat(safe).isTrue();
    }

    @Test
    void controllerUnreachableIsTreatedAsSafe() {
        boolean safe = checker.isControllerSafeToRestart("localhost:19093", 1999);
        assertThat(safe).isTrue();
    }
}
