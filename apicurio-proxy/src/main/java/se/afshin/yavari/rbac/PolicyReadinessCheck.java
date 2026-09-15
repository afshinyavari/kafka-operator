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
    @Inject KafkaAclPolicySource acls;

    /** Ready once the role policy file is loaded and, when the Kafka-ACL source is
     *  enabled, the first ACL snapshot has been fetched. */
    @Override
    public HealthCheckResponse call() {
        boolean up = engine.isInitialized() && (!acls.isEnabled() || acls.isLoaded());
        return up ? HealthCheckResponse.up("policy-loaded") : HealthCheckResponse.down("policy-loaded");
    }
}
