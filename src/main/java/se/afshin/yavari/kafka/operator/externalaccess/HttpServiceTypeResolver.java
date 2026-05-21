package se.afshin.yavari.kafka.operator.externalaccess;

import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;

/**
 * Maps {@link ExternalAccessType} → Kubernetes Service {@code spec.type} for HTTP services.
 *
 * <p>GATEWAY and INGRESS leave the Service as ClusterIP since the external traffic enters
 * through a Gateway or Ingress controller and is then routed to the in-cluster Service.
 */
public final class HttpServiceTypeResolver {

    private HttpServiceTypeResolver() {}

    public static String resolve(HttpExternalAccessConfig ea) {
        if (ea == null || ea.getType() == null) {
            return "ClusterIP";
        }
        return switch (ea.getType()) {
            case NODEPORT -> "NodePort";
            case LOADBALANCER -> "LoadBalancer";
            case GATEWAY, INGRESS -> "ClusterIP";
        };
    }
}
