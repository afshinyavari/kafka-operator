package se.afshin.yavari.clientapp.runtime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunnerRegistryTest {

    @Test
    void emptyRegistryIsNotAllStarted() {
        RunnerRegistry r = new RunnerRegistry();
        assertFalse(r.allStarted());
    }

    @Test
    void oneStartedRunnerIsAllStarted() {
        RunnerRegistry r = new RunnerRegistry();
        r.register(() -> true);
        assertTrue(r.allStarted());
    }

    @Test
    void oneStartedAndOneNotStartedIsNotAllStarted() {
        RunnerRegistry r = new RunnerRegistry();
        r.register(() -> true);
        r.register(() -> false);
        assertFalse(r.allStarted());
    }
}
