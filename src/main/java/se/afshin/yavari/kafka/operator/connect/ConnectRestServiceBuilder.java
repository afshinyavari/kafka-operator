package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;

import java.util.Map;

/**
 * Builds the {@code <name>-connect} ClusterIP Service fronting all Connect worker pods.
 * Distributed-mode Connect uses Kafka's group coordinator for membership, so a regular
 * ClusterIP service (not headless) is the right shape — REST clients reach any worker,
 * and the workers forward modifying ops to the current leader through the internal REST
 * URLs Connect auto-discovers.
 */
@ApplicationScoped
public class ConnectRestServiceBuilder {

    public static final String SUFFIX = "-connect";
    public static final String PORT_NAME = "rest";

    public Service build(KafkaConnect cr, OwnerReference ownerRef) {
        String name = cr.getMetadata().getName();
        Map<String, String> labels = ConnectLabels.labels(name);
        int port = cr.getSpec().getRestPort();
        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name + SUFFIX)
                    .withNamespace(cr.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withSelector(labels)
                    .addNewPort()
                        .withName(PORT_NAME)
                        .withPort(port)
                        .withTargetPort(new IntOrString(port))
                    .endPort()
                .endSpec()
                .build();
    }
}
