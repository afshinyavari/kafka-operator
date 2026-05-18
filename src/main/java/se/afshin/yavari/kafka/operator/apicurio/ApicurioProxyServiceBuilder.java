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
        var labels = ApicurioProxyDeploymentBuilder.labels(name);

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
                        .withPort(ApicurioProxyDeploymentBuilder.PROXY_PORT)
                        .withTargetPort(new IntOrString(ApicurioProxyDeploymentBuilder.PROXY_PORT))
                    .endPort()
                .endSpec()
                .build();
    }
}
