package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Declarative, scheduled backup of a managed {@link KafkaCluster}'s topic data
 * (and, optionally, its Apicurio schemas) to object storage. The reconciler renders
 * an osodevops kafka-backup config and builds a Kubernetes {@code CronJob}; Kubernetes
 * owns the schedule cadence.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kbk")
public class KafkaBackup extends CustomResource<KafkaBackupSpec, KafkaBackupStatus>
        implements Namespaced {

    /** Stable backup identifier passed to the kafka-backup tool — the CR name. */
    public String resolvedBackupId() {
        return getMetadata().getName();
    }
}
