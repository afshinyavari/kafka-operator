package se.afshin.yavari.kafka.editor.api;

/**
 * Kafka cluster + schema registry + Kafka Connect connection for a live run or
 * an admin call. Plaintext only for now — SASL/TLS will arrive in a later
 * milestone (and can be supplied via environment variables in the meantime).
 */
public record ConnectionConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String connectUrl) {

    public String bootstrapServersOrDefault() {
        return bootstrapServers != null && !bootstrapServers.isBlank()
                ? bootstrapServers.trim()
                : "localhost:9092";
    }

    public String schemaRegistryUrlOrNull() {
        return schemaRegistryUrl != null && !schemaRegistryUrl.isBlank()
                ? schemaRegistryUrl.trim()
                : null;
    }

    public String connectUrlOrNull() {
        return connectUrl != null && !connectUrl.isBlank()
                ? connectUrl.trim()
                : null;
    }
}
