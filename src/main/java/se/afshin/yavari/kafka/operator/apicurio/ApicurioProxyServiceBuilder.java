package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpServiceTypeResolver;

@ApplicationScoped
public class ApicurioProxyServiceBuilder {

    public Service build(ApicurioRegistry registry, String namespace) {
        String name = registry.getMetadata().getName();
        // Selects the merged registry+proxy pod via the registry's pod label.
        var labels = ApicurioDeploymentBuilder.labels(name);
        HttpExternalAccessConfig ea = registry.getSpec().getExternalAccess();

        var portBuilder = new ServicePortBuilder()
                .withName("http")
                .withPort(ApicurioProxyContainerBuilder.PROXY_PORT)
                .withTargetPort(new IntOrString(ApicurioProxyContainerBuilder.PROXY_PORT));
        if (ea != null && ea.getType() == ExternalAccessType.NODEPORT && ea.getNodePort() != null) {
            portBuilder.withNodePort(ea.getNodePort());
        }

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name + "-rbac-proxy")
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withType(HttpServiceTypeResolver.resolve(ea))
                    .withSelector(labels)
                    .withPorts(portBuilder.build())
                .endSpec()
                .build();
    }
}
