package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kcon")
public class KafkaConnector extends CustomResource<KafkaConnectorSpec, KafkaConnectorStatus>
        implements Namespaced {

    /** Returns the connector's identity on the Connect REST API: spec.connectorName if
     *  set, otherwise metadata.name. */
    public String resolvedConnectorName() {
        if (getSpec() != null
                && getSpec().getConnectorName() != null
                && !getSpec().getConnectorName().isBlank()) {
            return getSpec().getConnectorName();
        }
        return getMetadata().getName();
    }
}
