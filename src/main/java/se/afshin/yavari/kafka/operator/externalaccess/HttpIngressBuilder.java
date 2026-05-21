package se.afshin.yavari.kafka.operator.externalaccess;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLSBuilder;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;

/**
 * Builds a standard {@code networking.k8s.io/v1 Ingress} for an HTTP service.
 *
 * <p>Unlike the Kafka-proxy IngressBuilder this does NOT set {@code ssl-passthrough}; HTTP
 * traffic terminates at the controller. When {@link HttpIngressConfig#getTlsSecretRef()} is
 * set, a {@code spec.tls[]} entry references that Secret so the controller terminates TLS for
 * the advertised host.
 */
@ApplicationScoped
public class HttpIngressBuilder {

    public Ingress build(String name, String namespace, Map<String, String> labels,
                         OwnerReference ownerRef, String host, String serviceName, int servicePort,
                         HttpIngressConfig ing) {
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("advertised host is required to build an Ingress");
        }
        String ingressClass = ing != null ? ing.getIngressClassName() : null;
        String tlsSecret = ing != null ? ing.getTlsSecretRef() : null;
        Map<String, String> annotations = ing != null ? ing.getAnnotations() : Map.of();

        var builder = new IngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withIngressClassName(ingressClass)
                    .addNewRule()
                        .withHost(host)
                        .withNewHttp()
                            .addNewPath()
                                .withPath("/")
                                .withPathType("Prefix")
                                .withNewBackend()
                                    .withNewService()
                                        .withName(serviceName)
                                        .withNewPort().withNumber(servicePort).endPort()
                                    .endService()
                                .endBackend()
                            .endPath()
                        .endHttp()
                    .endRule()
                .endSpec();

        if (tlsSecret != null && !tlsSecret.isBlank()) {
            builder.editSpec()
                    .withTls(new IngressTLSBuilder()
                            .withHosts(host)
                            .withSecretName(tlsSecret)
                            .build())
                    .endSpec();
        }
        return builder.build();
    }
}
