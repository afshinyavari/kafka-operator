package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Declarative Cruise Control rebalance request. The operator generates an optimization
 * proposal (dry run), surfaces it on {@code status.optimizationResult}, and — once the user
 * approves it via the {@value #REBALANCE_ANNOTATION} annotation — executes the rebalance and
 * tracks it to completion.
 *
 * <p>Approval is annotation-driven (mirroring Strimzi): set
 * {@code kafka.yavari.afshin.se/rebalance=approve} to execute a ready proposal,
 * {@code =refresh} to regenerate it, {@code =stop} to abort. The operator consumes and
 * clears the annotation.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("krb")
public class KafkaRebalance extends CustomResource<KafkaRebalanceSpec, KafkaRebalanceStatus>
        implements Namespaced {

    /** Annotation that drives the proposal/approve/execute state machine. */
    public static final String REBALANCE_ANNOTATION = "kafka.yavari.afshin.se/rebalance";

    public static final String APPROVE = "approve";
    public static final String REFRESH = "refresh";
    public static final String STOP = "stop";

    /** @return the current value of {@value #REBALANCE_ANNOTATION}, or null if absent. */
    public String pendingAnnotation() {
        if (getMetadata() == null || getMetadata().getAnnotations() == null) return null;
        String v = getMetadata().getAnnotations().get(REBALANCE_ANNOTATION);
        return (v == null || v.isBlank()) ? null : v.trim().toLowerCase();
    }
}
