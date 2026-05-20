package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLSBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIIngressConfig;

@ApplicationScoped
public class UIIngressBuilder {

    public Ingress build(KafkaUI ui, OwnerReference ownerRef) {
        String name = ui.getMetadata().getName();
        String namespace = ui.getMetadata().getNamespace();
        KafkaUIIngressConfig cfg = ui.getSpec().getIngress();
        int port = ui.getSpec().getService().getPort();

        var ingressBuilder = new IngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(UILabels.labels(name))
                    .withAnnotations(cfg.getAnnotations())
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withIngressClassName(cfg.getClassName())
                    .addNewRule()
                        .withHost(cfg.getHost())
                        .withNewHttp()
                            .addNewPath()
                                .withPath("/")
                                .withPathType("Prefix")
                                .withNewBackend()
                                    .withNewService()
                                        .withName(name)
                                        .withNewPort().withNumber(port).endPort()
                                    .endService()
                                .endBackend()
                            .endPath()
                        .endHttp()
                    .endRule()
                .endSpec();

        if (cfg.getTlsSecret() != null && !cfg.getTlsSecret().isEmpty()) {
            ingressBuilder.editSpec()
                    .withTls(new IngressTLSBuilder()
                            .withHosts(cfg.getHost())
                            .withSecretName(cfg.getTlsSecret())
                            .build())
                    .endSpec();
        }
        return ingressBuilder.build();
    }
}
