package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;

@ApplicationScoped
public class ApicurioServiceBuilder {

    public Service build(ApicurioRegistry registry, String namespace) {
        String name = registry.getMetadata().getName();
        var labels = ApicurioDeploymentBuilder.labels(name);

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name + "-registry")
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withSelector(labels)
                    .addNewPort()
                        .withName("http")
                        .withPort(ApicurioDeploymentBuilder.REGISTRY_PORT)
                        .withTargetPort(new IntOrString(ApicurioDeploymentBuilder.REGISTRY_PORT))
                    .endPort()
                .endSpec()
                .build();
    }
}
