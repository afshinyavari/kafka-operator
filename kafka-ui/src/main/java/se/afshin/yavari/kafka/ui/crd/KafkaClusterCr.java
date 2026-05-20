package se.afshin.yavari.kafka.ui.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

import java.util.Map;

/**
 * Local mirror of the operator's KafkaCluster CRD. Only fields the UI actually
 * reads are modelled (everything else lands in {@code spec.additionalProperties}
 * and is ignored). This avoids depending on the operator JAR.
 *
 * <p>The annotations explicitly set kind/plural/singular to match the operator's
 * CRD — fabric8's default derivation from the class name would otherwise produce
 * {@code kafkaclustercrs}, which does not match the registered CRD.
 */
@Group("kafka.yavari.afshin.se")
@Version("v1alpha1")
@Kind("KafkaCluster")
@Plural("kafkaclusters")
@Singular("kafkacluster")
public class KafkaClusterCr extends CustomResource<Map<String, Object>, Map<String, Object>>
        implements Namespaced {
}
