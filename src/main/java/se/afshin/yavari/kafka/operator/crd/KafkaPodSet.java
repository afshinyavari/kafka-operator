package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * Operator-managed CRD for a set of individual Kafka pods.
 * Created and owned by KafkaNodePool — not intended for direct user manipulation.
 */
@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kps")
public class KafkaPodSet extends CustomResource<KafkaPodSetSpec, KafkaPodSetStatus>
        implements Namespaced {

    public static final String SPEC_HASH_ANNOTATION     = "kafka.yavari.afshin.se/spec-hash";
    public static final String KAFKA_VERSION_ANNOTATION = "kafka.yavari.afshin.se/kafka-version";
    public static final String NODE_ID_LABEL            = "kafka.yavari.afshin.se/node-id";
    public static final String NODE_POOL_LABEL          = "kafka.yavari.afshin.se/node-pool";
    public static final String CLUSTER_LABEL            = "kafka.yavari.afshin.se/cluster";
    public static final String MANAGED_BY_LABEL         = "app.kubernetes.io/managed-by";
    public static final String MANAGED_BY_VALUE         = "kafka-operator";
}
