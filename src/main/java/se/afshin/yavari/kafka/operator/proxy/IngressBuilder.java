package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressPathBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.HTTPIngressRuleValueBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBackendBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressServiceBackendBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyIngressConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@code networking.k8s.io/v1 Ingress} for the proxy with nginx-style ssl-passthrough.
 *
 * <p>Each host (bootstrap + one per broker) gets its own rule. Path-based routing is irrelevant
 * for ssl-passthrough — nginx-ingress dispatches at the TCP layer based on TLS SNI — but the
 * Ingress schema still requires at least one HTTP path per rule, so we point all of them at the
 * proxy Service on {@code spec.clientPort}.
 *
 * <p>The ingress controller must support SSL passthrough: nginx-ingress with the
 * {@code --enable-ssl-passthrough} flag, or another controller with equivalent capability.
 */
@ApplicationScoped
public class IngressBuilder {

    static final String SSL_PASSTHROUGH_ANNOTATION = "nginx.ingress.kubernetes.io/ssl-passthrough";
    static final String BACKEND_PROTOCOL_ANNOTATION = "nginx.ingress.kubernetes.io/backend-protocol";

    public Ingress build(KafkaProxy proxy, int brokerCount, String namespace,
                         ExternalAccessResolution external) {
        String name = proxy.getMetadata().getName();
        int clientPort = proxy.getSpec().getClientPort();
        KafkaProxyExternalAccessConfig ea = proxy.getSpec().getExternalAccess();
        KafkaProxyIngressConfig ing = ea != null ? ea.getIngress() : null;

        String advertisedHost = external.advertisedHost();
        if (advertisedHost == null || advertisedHost.isBlank()) {
            throw new IllegalStateException(
                    "advertisedHost must be resolved before building Ingress");
        }

        List<IngressRule> rules = buildRules(proxy, brokerCount, advertisedHost, name, clientPort);

        var ingressBuilder = new io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(ProxyDeploymentBuilder.labels(name))
                    .withAnnotations(Map.of(
                            SSL_PASSTHROUGH_ANNOTATION, "true",
                            BACKEND_PROTOCOL_ANNOTATION, "HTTPS"))
                .endMetadata()
                .withNewSpec()
                    .withRules(rules)
                .endSpec();

        if (ing != null && ing.getIngressClassName() != null && !ing.getIngressClassName().isBlank()) {
            ingressBuilder.editSpec().withIngressClassName(ing.getIngressClassName()).endSpec();
        }

        return ingressBuilder.build();
    }

    private static List<IngressRule> buildRules(KafkaProxy proxy, int brokerCount,
                                                 String advertisedHost, String serviceName, int port) {
        List<IngressRule> rules = new ArrayList<>();
        rules.add(rule("bootstrap." + advertisedHost, serviceName, port));

        List<BrokerNodeIdRange> ranges = proxy.getSpec().getBrokerNodeIdRanges();
        if (ranges != null && !ranges.isEmpty()) {
            for (BrokerNodeIdRange r : ranges) {
                for (int id = r.getStart(); id <= r.getEnd(); id++) {
                    rules.add(rule("broker-" + id + "." + advertisedHost, serviceName, port));
                }
            }
        } else {
            for (int i = 0; i < brokerCount; i++) {
                rules.add(rule("broker-" + i + "." + advertisedHost, serviceName, port));
            }
        }
        return rules;
    }

    private static IngressRule rule(String host, String serviceName, int port) {
        var backend = new IngressBackendBuilder()
                .withService(new IngressServiceBackendBuilder()
                        .withName(serviceName)
                        .withNewPort().withNumber(port).endPort()
                        .build())
                .build();
        var path = new HTTPIngressPathBuilder()
                .withPath("/")
                .withPathType("Prefix")
                .withBackend(backend)
                .build();
        var httpRule = new HTTPIngressRuleValueBuilder()
                .withPaths(List.of(path))
                .build();
        return new IngressRuleBuilder()
                .withHost(host)
                .withHttp(httpRule)
                .build();
    }
}
