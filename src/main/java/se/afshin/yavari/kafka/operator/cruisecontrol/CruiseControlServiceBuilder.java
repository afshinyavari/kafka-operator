package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Builds the in-cluster {@code cruise-control} ClusterIP Service exposing the Cruise
 * Control REST API. v1 is in-cluster only — the {@code KafkaRebalance} reconciler reaches
 * it over this Service; operators can {@code kubectl port-forward} for ad-hoc access.
 */
@ApplicationScoped
public class CruiseControlServiceBuilder {

    public Service build(String namespace) {
        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(CruiseControlOrchestrator.CC_NAME)
                    .withNamespace(namespace)
                    .withLabels(CruiseControlDeploymentBuilder.labels())
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withSelector(CruiseControlDeploymentBuilder.labels())
                    .addNewPort()
                        .withName("cc-rest")
                        .withProtocol("TCP")
                        .withPort(CruiseControlOrchestrator.REST_PORT)
                        .withTargetPort(new IntOrString(CruiseControlOrchestrator.REST_PORT))
                    .endPort()
                .endSpec()
                .build();
    }
}
