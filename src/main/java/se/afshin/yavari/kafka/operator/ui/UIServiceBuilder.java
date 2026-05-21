package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpServiceTypeResolver;

@ApplicationScoped
public class UIServiceBuilder {

    public Service build(KafkaUI ui, OwnerReference ownerRef) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        HttpExternalAccessConfig ea = ui.getSpec().getExternalAccess();
        var labels = UILabels.labels(name);

        var portBuilder = new ServicePortBuilder()
                .withName("http")
                .withPort(KafkaUISpec.PORT)
                .withTargetPort(new IntOrString(KafkaUISpec.PORT))
                .withProtocol("TCP");
        if (ea != null && ea.getType() == ExternalAccessType.NODEPORT && ea.getNodePort() != null) {
            portBuilder = portBuilder.withNodePort(ea.getNodePort());
        }

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withType(HttpServiceTypeResolver.resolve(ea))
                    .withSelector(labels)
                    .withPorts(portBuilder.build())
                .endSpec()
                .build();
    }
}
