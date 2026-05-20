package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;

import java.util.List;

/**
 * Resolves the externally-advertised hostname for a {@link KafkaProxy} on the local K8s cluster.
 *
 * <p>Each cluster's operator runs its own reconciler instance with its own
 * {@code localClusterId} — so the same KafkaProxy CR produces different external resolutions
 * on different clusters.
 *
 * <ul>
 *   <li>{@code LOADBALANCER}: read {@code Service.status.loadBalancer.ingress[0]} on the current
 *   cluster; falls back to {@code advertisedHostTemplate} (with {@code ${clusterId}} substitution)
 *   if the user explicitly provided one.
 *   <li>{@code GATEWAY} / {@code INGRESS}: substitute {@code ${clusterId}} in
 *   {@code advertisedHostTemplate}.
 * </ul>
 */
@ApplicationScoped
public class ExternalAccessResolver {

    public ExternalAccessResolution resolve(KafkaProxy proxy, String localClusterId,
                                            String namespace, KubernetesClient client) {
        KafkaProxyExternalAccessConfig ea = proxy.getSpec().getExternalAccess();
        if (ea == null || ea.getType() == null) {
            return ExternalAccessResolution.internal();
        }

        ExternalAccessType type = ea.getType();
        String template = ea.getAdvertisedHostTemplate();

        switch (type) {
            case LOADBALANCER -> {
                if (template != null && !template.isBlank()) {
                    return ExternalAccessResolution.resolved(type, substitute(template, localClusterId));
                }
                String lbHost = readLoadBalancerIngress(client, namespace, proxy.getMetadata().getName());
                if (lbHost == null) {
                    return ExternalAccessResolution.pending(type);
                }
                return ExternalAccessResolution.resolved(type, lbHost);
            }
            case GATEWAY, INGRESS -> {
                if (template == null || template.isBlank()) {
                    throw new IllegalStateException("externalAccess.advertisedHostTemplate is required for type=" + type);
                }
                return ExternalAccessResolution.resolved(type, substitute(template, localClusterId));
            }
            default -> throw new IllegalStateException(
                    "externalAccess.type=" + type + " is not supported on the proxy");
        }
    }

    static String substitute(String template, String localClusterId) {
        String id = localClusterId == null ? "" : localClusterId.toLowerCase();
        return template.replace("${clusterId}", id);
    }

    private String readLoadBalancerIngress(KubernetesClient client, String namespace, String serviceName) {
        Service svc = client.services().inNamespace(namespace).withName(serviceName).get();
        if (svc == null || svc.getStatus() == null || svc.getStatus().getLoadBalancer() == null) {
            return null;
        }
        List<LoadBalancerIngress> ingress = svc.getStatus().getLoadBalancer().getIngress();
        if (ingress == null || ingress.isEmpty()) {
            return null;
        }
        LoadBalancerIngress first = ingress.get(0);
        return first.getHostname() != null ? first.getHostname() : first.getIp();
    }
}
