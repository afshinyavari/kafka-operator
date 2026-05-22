package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * One-shot restore of a {@link KafkaBackup}'s data into a managed {@link KafkaCluster}.
 * The reconciler builds a Kubernetes {@code Job} exactly once and is idempotent: once
 * {@code status.phase} reaches a terminal value the Job is never re-created. Re-running
 * a restore requires deleting and re-creating the CR.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("krs")
public class KafkaRestore extends CustomResource<KafkaRestoreSpec, KafkaRestoreStatus>
        implements Namespaced {
}
