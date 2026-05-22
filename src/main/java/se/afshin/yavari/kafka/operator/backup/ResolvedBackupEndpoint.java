package se.afshin.yavari.kafka.operator.backup;

/**
 * Concrete connection details for a managed {@code KafkaCluster}, resolved by
 * {@link BackupEndpointResolver}. Backup/restore workloads connect <strong>direct to
 * the broker headless service</strong> on the internal listener — not through the
 * Kroxylicious proxy — so bulk full-topic reads stay off the shared proxy.
 *
 * @param bootstrap     broker headless service address ({@code host:9092})
 * @param tlsSecretRef  PEM-shaped Secret ({@code tls.crt}/{@code tls.key}/{@code ca.crt})
 *                      when the cluster has {@code proxyMtls}; null for plaintext
 * @param apicurioUrl   base URL of the cluster's Apicurio registry, or null
 */
public record ResolvedBackupEndpoint(String bootstrap, String tlsSecretRef, String apicurioUrl) {

    public boolean hasTls() { return tlsSecretRef != null; }

    public boolean hasApicurio() { return apicurioUrl != null; }
}
