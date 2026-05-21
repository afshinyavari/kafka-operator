package se.afshin.yavari.kafka.operator.externalaccess;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a {@code gateway.networking.k8s.io/v1 HTTPRoute} pointing at the target Service.
 *
 * <p>{@link GenericKubernetesResource} is used to avoid pulling in a typed Gateway-API model
 * dependency — matches the pattern already used by the Kafka-proxy TLSRouteBuilder.
 */
@ApplicationScoped
public class HttpRouteBuilder {

    public static final String API_VERSION = "gateway.networking.k8s.io/v1";
    public static final String KIND = "HTTPRoute";

    public GenericKubernetesResource build(String name, String namespace, Map<String, String> labels,
                                           OwnerReference ownerRef, String host, String serviceName,
                                           int servicePort, HttpGatewayConfig gw) {
        if (gw == null || gw.getParentGatewayName() == null || gw.getParentGatewayName().isBlank()) {
            throw new IllegalStateException(
                    "externalAccess.gateway.parentGatewayName is required for type=GATEWAY");
        }
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("advertised host is required to build an HTTPRoute");
        }

        Map<String, Object> parentRef = new HashMap<>();
        parentRef.put("name", gw.getParentGatewayName());
        parentRef.put("namespace", gw.getParentGatewayNamespace() != null
                ? gw.getParentGatewayNamespace() : namespace);
        if (gw.getSectionName() != null && !gw.getSectionName().isBlank()) {
            parentRef.put("sectionName", gw.getSectionName());
        }

        Map<String, Object> match = new HashMap<>();
        Map<String, Object> path = new HashMap<>();
        path.put("type", "PathPrefix");
        path.put("value", "/");
        match.put("path", path);

        Map<String, Object> backendRef = new HashMap<>();
        backendRef.put("name", serviceName);
        backendRef.put("port", servicePort);

        Map<String, Object> rule = new HashMap<>();
        rule.put("matches", List.of(match));
        rule.put("backendRefs", List.of(backendRef));

        Map<String, Object> spec = new HashMap<>();
        spec.put("parentRefs", List.of(parentRef));
        spec.put("hostnames", List.of(host));
        spec.put("rules", List.of(rule));

        GenericKubernetesResource route = new GenericKubernetesResource();
        route.setApiVersion(API_VERSION);
        route.setKind(KIND);
        var meta = new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace)
                .withLabels(labels);
        if (ownerRef != null) {
            meta.withOwnerReferences(ownerRef);
        }
        route.setMetadata(meta.build());
        route.setAdditionalProperty("spec", spec);
        return route;
    }
}
