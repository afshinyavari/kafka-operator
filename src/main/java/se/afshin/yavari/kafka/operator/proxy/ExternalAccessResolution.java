package se.afshin.yavari.kafka.operator.proxy;

import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;

/**
 * Resolved external-access state for a single reconcile pass on one K8s cluster.
 *
 * <p>{@link #internal()} = no external access configured; ClusterIP defaults apply.
 * <p>{@link #pending()} = LoadBalancer ingress not yet assigned; reconciler should reschedule.
 * <p>Otherwise, {@link #advertisedHost()} is the externally-reachable hostname/IP that the
 * proxy should advertise to Kafka clients.
 */
public final class ExternalAccessResolution {

    private final ExternalAccessType type;
    private final String advertisedHost;
    private final boolean pending;

    private ExternalAccessResolution(ExternalAccessType type, String advertisedHost, boolean pending) {
        this.type = type;
        this.advertisedHost = advertisedHost;
        this.pending = pending;
    }

    /** No externalAccess configured. ProxyService stays ClusterIP, Kroxy uses internal DNS. */
    public static ExternalAccessResolution internal() {
        return new ExternalAccessResolution(null, null, false);
    }

    /** LB ingress not yet visible. Caller should patch status and reschedule. */
    public static ExternalAccessResolution pending(ExternalAccessType type) {
        return new ExternalAccessResolution(type, null, true);
    }

    /** External access is configured and the advertised host has been resolved. */
    public static ExternalAccessResolution resolved(ExternalAccessType type, String advertisedHost) {
        return new ExternalAccessResolution(type, advertisedHost, false);
    }

    public ExternalAccessType type() { return type; }
    public String advertisedHost() { return advertisedHost; }
    public boolean isPending() { return pending; }
    public boolean isInternal() { return type == null; }
}
