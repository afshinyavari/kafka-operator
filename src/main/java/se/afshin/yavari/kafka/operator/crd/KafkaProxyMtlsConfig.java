package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyMtlsConfig {

    public static final String DEFAULT_ADMIN_CLIENT_CERT_SECRET = "kafka-operator-client-tls";

    private boolean enabled;
    private String proxyPrincipal = "kafka-proxy";

    /** Name of a Secret (kubernetes.io/tls convention: tls.crt, tls.key, ca.crt) in the
     *  same namespace, holding a client cert signed by the shared CA. Used by
     *  AdminClient consumers in the operator (currently the KafkaTopic reconciler) to
     *  reach the broker INTERNAL listener when mTLS is enabled. CN must be in broker
     *  super.users — by convention reuse {@code proxyPrincipal}. Defaults to
     *  {@value DEFAULT_ADMIN_CLIENT_CERT_SECRET} when unset. */
    private String adminClientCertSecretRef;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

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
