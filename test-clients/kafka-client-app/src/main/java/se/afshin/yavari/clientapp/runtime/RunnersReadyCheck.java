package se.afshin.yavari.clientapp.runtime;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class RunnersReadyCheck implements HealthCheck {
    @Inject
    RunnerRegistry registry;

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("runners").status(registry.allStarted()).build();
    }
}
