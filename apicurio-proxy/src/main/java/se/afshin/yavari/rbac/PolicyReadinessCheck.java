package se.afshin.yavari.rbac;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class PolicyReadinessCheck implements HealthCheck {

    @Inject PolicyEngine engine;

    @Override
    public HealthCheckResponse call() {
        return engine.isInitialized()
            ? HealthCheckResponse.up("policy-loaded")
            : HealthCheckResponse.down("policy-loaded");
    }
}
