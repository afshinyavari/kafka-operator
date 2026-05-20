package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Version("v1alpha1")
@Group("kafka.yavari.afshin.se")
@ShortNames("kt")
public class KafkaTopic extends CustomResource<KafkaTopicSpec, KafkaTopicStatus>
        implements Namespaced {

    /** Returns the actual Kafka topic name: spec.topicName if set, otherwise metadata.name. */
    public String resolvedTopicName() {
        if (getSpec() != null && getSpec().getTopicName() != null && !getSpec().getTopicName().isBlank()) {
            return getSpec().getTopicName();
        }
        return getMetadata().getName();
    }
}
