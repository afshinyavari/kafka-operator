package se.afshin.yavari.kafka.operator.crd;

/**
 * References pre-existing TLS secrets the proxy mounts. The operator does not
 * create or sign these — they must be provisioned externally (cert-manager in
 * production, mcs-setup.sh in tests). Both secrets follow the cert-manager
 * convention: kubernetes.io/tls shape with tls.crt + tls.key + ca.crt keys.
 *
 * When fields are null, the reconciler falls back to convention defaults of
 * "{proxyName}-client-tls" and "{proxyName}-server-tls" so a typical KafkaProxy
 * CR needs no `tls:` block at all.
 */
public class KafkaProxyTlsConfig {

    /** Secret holding the proxy's client cert (presented to brokers on upstream mTLS). */
    private String clientCertSecretRef;

    /** Secret holding the proxy's server cert (presented to clients connecting to the gateway). */
    private String serverCertSecretRef;

    public String getClientCertSecretRef() { return clientCertSecretRef; }
    public void setClientCertSecretRef(String clientCertSecretRef) { this.clientCertSecretRef = clientCertSecretRef; }

    public String getServerCertSecretRef() { return serverCertSecretRef; }
    public void setServerCertSecretRef(String serverCertSecretRef) { this.serverCertSecretRef = serverCertSecretRef; }
}
