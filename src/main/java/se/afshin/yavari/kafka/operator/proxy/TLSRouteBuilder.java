package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyGatewayConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@code gateway.networking.k8s.io/v1alpha2 TLSRoute} pointing at the proxy Service.
 *
 * <p>One TLSRoute carries the bootstrap hostname plus one per-broker hostname. Each hostname
 * is dispatched by the Gateway via TLS SNI passthrough to the proxy's single listen port
 * ({@code spec.clientPort}). The proxy ({@code sniHostIdentifiesNode} mode) then routes to the
 * correct broker based on the SNI.
 *
 * <p>{@link GenericKubernetesResource} is used (rather than a typed model class) to avoid
 * pulling in the fabric8 Gateway-API model dependency — matches the pattern already used for
 * the multicluster {@code ServiceExport} resource.
 */
@ApplicationScoped
public class TLSRouteBuilder {

    static final String API_VERSION = "gateway.networking.k8s.io/v1alpha2";
    static final String KIND = "TLSRoute";

    public GenericKubernetesResource build(KafkaProxy proxy, int brokerCount, String namespace,
                                            ExternalAccessResolution external) {
        String name = proxy.getMetadata().getName();
        int clientPort = proxy.getSpec().getClientPort();
        KafkaProxyExternalAccessConfig ea = proxy.getSpec().getExternalAccess();
        KafkaProxyGatewayConfig gw = ea != null ? ea.getGateway() : null;
        if (gw == null || gw.getParentGatewayName() == null || gw.getParentGatewayName().isBlank()) {
            throw new IllegalStateException(
                    "externalAccess.gateway.parentGatewayName is required for type=GATEWAY");
        }

        String advertisedHost = external.advertisedHost();
        if (advertisedHost == null || advertisedHost.isBlank()) {
            throw new IllegalStateException(
                    "advertisedHost must be resolved before building TLSRoute");
        }

        List<String> hostnames = hostnames(proxy, brokerCount, advertisedHost);

        Map<String, Object> parentRef = new HashMap<>();
        parentRef.put("name", gw.getParentGatewayName());
        parentRef.put("namespace", gw.getParentGatewayNamespace() != null
                ? gw.getParentGatewayNamespace()
                : namespace);
        if (gw.getSectionName() != null && !gw.getSectionName().isBlank()) {
            parentRef.put("sectionName", gw.getSectionName());
        }

        Map<String, Object> backendRef = new HashMap<>();
        backendRef.put("name", name);
        backendRef.put("port", clientPort);

        Map<String, Object> rule = new HashMap<>();
        rule.put("backendRefs", List.of(backendRef));

        Map<String, Object> spec = new HashMap<>();
        spec.put("parentRefs", List.of(parentRef));
        spec.put("hostnames", hostnames);
        spec.put("rules", List.of(rule));

        GenericKubernetesResource route = new GenericKubernetesResource();
        route.setApiVersion(API_VERSION);
        route.setKind(KIND);
        route.setMetadata(new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(ProxyDeploymentBuilder.labels(name))
                .build());
        route.setAdditionalProperty("spec", spec);
        return route;
    }

    static List<String> hostnames(KafkaProxy proxy, int brokerCount, String advertisedHost) {
        List<String> out = new ArrayList<>();
        out.add("bootstrap." + advertisedHost);
        List<BrokerNodeIdRange> ranges = proxy.getSpec().getBrokerNodeIdRanges();
        if (ranges != null && !ranges.isEmpty()) {
            for (BrokerNodeIdRange r : ranges) {
                for (int id = r.getStart(); id <= r.getEnd(); id++) {
                    out.add("broker-" + id + "." + advertisedHost);
                }
            }
        } else {
            for (int i = 0; i < brokerCount; i++) {
                out.add("broker-" + i + "." + advertisedHost);
            }
        }
        return out;
    }
}
