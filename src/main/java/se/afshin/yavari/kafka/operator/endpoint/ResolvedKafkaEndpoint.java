package se.afshin.yavari.kafka.operator.endpoint;

/**
 * Concrete endpoint description after resolving a
 * {@link se.afshin.yavari.kafka.operator.crd.KafkaEndpoint}.
 *
 * <p>For managed endpoints, fields are derived from the referenced {@code KafkaCluster}'s
 * proxy + Apicurio config; for external endpoints, fields come straight from the spec.
 *
 * <p>{@code tlsSecretRef} is the name of a PEM-shaped Secret ({@code tls.crt}/{@code tls.key}/
 * {@code ca.crt}) in the consuming CR's namespace.
 */
public record ResolvedKafkaEndpoint(
        String bootstrap,
        String tlsSecretRef,
        Sasl sasl,
        String schemaRegistryUrl,
        String schemaRegistryAuthSecretRef,
        boolean schemaRegistryConfluent
) {
    public record Sasl(String mechanism, String secretRef) {}

    public boolean hasTls() { return tlsSecretRef != null; }
    public boolean hasSasl() { return sasl != null; }
    public boolean hasSchemaRegistry() { return schemaRegistryUrl != null; }
}
