package se.afshin.yavari.kafka.operator.infra;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

/**
 * Lifecycle observers so the operator's start/stop boundaries land in the logs.
 * The actual reconcile drain is driven by Quarkus shutdown -> JOSDK
 * {@code Operator.stop()}, bounded by {@code quarkus.shutdown.timeout} (60s in
 * {@code application.properties}). Setting a {@code terminationGracePeriodSeconds}
 * larger than that on the operator Deployment lets in-flight reconciles finish.
 */
@ApplicationScoped
public class OperatorLifecycle {

    private static final Logger LOG = Logger.getLogger(OperatorLifecycle.class);

    void onStart(@Observes StartupEvent event) {
        LOG.info("kafka-operator starting");
    }

    void onStop(@Observes ShutdownEvent event) {
        LOG.info("kafka-operator shutting down — draining in-flight reconciles "
                + "(bounded by quarkus.shutdown.timeout); leader-election lease will be released");
    }
}
