package se.afshin.yavari.kafka.operator.mm2;

/**
 * Concrete endpoint description after resolving an {@link se.afshin.yavari.kafka.operator.crd.Mm2Endpoint}.
 * For managed endpoints, fields are derived from the referenced {@code KafkaCluster}'s
 * proxy + Apicurio config; for external endpoints, fields come straight from the spec.
 *
 * <p>{@code tlsSecretRef} is the name of a PEM-shaped Secret ({@code tls.crt}/{@code tls.key}/
 * {@code ca.crt}) in the MM2 CR's namespace. For managed endpoints in a different namespace,
 * the operator copies the necessary Secret into the MM2 namespace (not in v1 — current
 * v1 requires source/target managed clusters to be in the same namespace as the MM2 CR).
 */
public record ResolvedEndpoint(
        String bootstrap,
        String tlsSecretRef,
        Mm2Sasl sasl,
        String schemaRegistryUrl,
        String schemaRegistryAuthSecretRef,
        boolean schemaRegistryConfluent
) {
    public record Mm2Sasl(String mechanism, String secretRef) {}

    public boolean hasTls() { return tlsSecretRef != null; }
    public boolean hasSasl() { return sasl != null; }
    public boolean hasSchemaRegistry() { return schemaRegistryUrl != null; }
}
