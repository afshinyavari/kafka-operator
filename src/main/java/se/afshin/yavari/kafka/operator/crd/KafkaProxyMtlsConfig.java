package se.afshin.yavari.kafka.operator.crd;

/**
 * mTLS configuration for the broker INTERNAL listener (the Kroxylicious-proxy upstream).
 * Setting {@code spec.proxyMtls} at all opts the cluster into mTLS — there's no separate
 * {@code enabled} toggle. To turn mTLS off, omit {@code proxyMtls} from the spec.
 *
 * <p>When set: INTERNAL listener is SSL + mutualTLS; broker StandardAuthorizer is loaded;
 * the broker pool's CN (under {@code super.users}) and the proxy's CN ({@code proxyPrincipal})
 * are the only super-users. Per-pool broker certs come from a Secret named
 * {@code {poolName}-broker-tls} (overridable via {@code KafkaNodePoolSpec.brokerCertSecretRef}).
 */
public class KafkaProxyMtlsConfig {

    public static final String DEFAULT_ADMIN_CLIENT_CERT_SECRET = "kafka-operator-client-tls";

    private String proxyPrincipal = "kafka-proxy";

    /** Name of a Secret (kubernetes.io/tls convention: tls.crt, tls.key, ca.crt) in the
     *  same namespace, holding a client cert signed by the shared CA. Used by
     *  AdminClient consumers in the operator (currently the KafkaTopic reconciler) to
     *  reach the broker INTERNAL listener. CN must be in broker super.users — by
     *  convention reuse {@code proxyPrincipal}. Defaults to
     *  {@value DEFAULT_ADMIN_CLIENT_CERT_SECRET} when unset. */
    private String adminClientCertSecretRef;

    public String getProxyPrincipal() { return proxyPrincipal; }
    public void setProxyPrincipal(String proxyPrincipal) { this.proxyPrincipal = proxyPrincipal; }

    public String getAdminClientCertSecretRef() { return adminClientCertSecretRef; }
    public void setAdminClientCertSecretRef(String adminClientCertSecretRef) {
        this.adminClientCertSecretRef = adminClientCertSecretRef;
    }

    /** Effective Secret name with convention fallback. */
    public String resolveAdminClientCertSecret() {
        return (adminClientCertSecretRef != null && !adminClientCertSecretRef.isBlank())
                ? adminClientCertSecretRef
                : DEFAULT_ADMIN_CLIENT_CERT_SECRET;
    }
}
