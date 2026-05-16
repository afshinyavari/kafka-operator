package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kc")
public class KafkaCluster extends CustomResource<KafkaClusterSpec, KafkaClusterStatus>
        implements Namespaced {
}
