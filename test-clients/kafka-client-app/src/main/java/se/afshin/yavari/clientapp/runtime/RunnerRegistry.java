package se.afshin.yavari.clientapp.runtime;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/** Started-state of every enabled runner, for the readiness probe. */
@ApplicationScoped
public class RunnerRegistry {
    private final List<BooleanSupplier> runners = new CopyOnWriteArrayList<>();

    public void register(BooleanSupplier started) { runners.add(started); }

    /** True once every registered runner reports started (and at least one is registered). */
    public boolean allStarted() {
        return !runners.isEmpty() && runners.stream().allMatch(BooleanSupplier::getAsBoolean);
    }
}
