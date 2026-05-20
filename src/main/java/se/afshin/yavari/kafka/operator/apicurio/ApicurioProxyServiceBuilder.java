package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;

@ApplicationScoped
public class ApicurioProxyServiceBuilder {

    public Service build(ApicurioRegistry registry, String namespace) {
        String name = registry.getMetadata().getName();
        // Selects the merged registry+proxy pod via the registry's pod label.
        var labels = ApicurioDeploymentBuilder.labels(name);

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name + "-rbac-proxy")
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withSelector(labels)
                    .addNewPort()
                        .withName("http")
                        .withPort(ApicurioProxyContainerBuilder.PROXY_PORT)
                        .withTargetPort(new IntOrString(ApicurioProxyContainerBuilder.PROXY_PORT))
                    .endPort()
                .endSpec()
                .build();
    }
}
