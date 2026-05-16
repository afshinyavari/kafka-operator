package se.afshin.yavari.kafka.operator;

import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Provides the LeaderElectionConfiguration CDI bean required by the JOSDK Quarkus extension.
 * The extension calls arc.instance(LeaderElectionConfiguration.class).get() at startup
 * when activate-leader-election-for-profiles matches the active profile, but does not
 * register a default bean itself — this producer fills that gap.
 */
@ApplicationScoped
public class LeaderElectionProducer {

    @Produces
    @ApplicationScoped
    public LeaderElectionConfiguration leaderElectionConfiguration() {
        // Lease namespace left unset: JOSDK auto-detects from the pod's namespace.
        return new LeaderElectionConfiguration("kafka-operator");
    }
}
