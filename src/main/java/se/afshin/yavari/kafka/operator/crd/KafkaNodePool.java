package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * User-facing CRD for a pool of Kafka nodes on the local cluster.
 * Must carry the label kafka.yavari.afshin.se/cluster={clusterName} to link to a KafkaCluster.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("knp")
public class KafkaNodePool extends CustomResource<KafkaNodePoolSpec, KafkaNodePoolStatus>
        implements Namespaced {

    public static final String CLUSTER_LABEL = "kafka.yavari.afshin.se/cluster";
}
