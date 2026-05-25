package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kcc")
public class KafkaConnect extends CustomResource<KafkaConnectSpec, KafkaConnectStatus>
        implements Namespaced {

    /** Returns the effective Connect {@code group.id}: spec.groupId if set, otherwise
     *  {@code connect-<metadata.name>}. */
    public String resolvedGroupId() {
        if (getSpec() != null
                && getSpec().getGroupId() != null
                && !getSpec().getGroupId().isBlank()) {
            return getSpec().getGroupId();
        }
        return "connect-" + getMetadata().getName();
    }
}
