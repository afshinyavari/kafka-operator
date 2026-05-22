package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * One-shot integrity check of a stored backup. The reconciler builds a Kubernetes
 * {@code Job} that runs {@code kafka-backup validate}; the terminal phase reflects
 * whether the backup is restorable. Idempotent like {@link KafkaRestore}.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kbv")
public class KafkaBackupValidation
        extends CustomResource<KafkaBackupValidationSpec, KafkaBackupValidationStatus>
        implements Namespaced {
}
