package se.afshin.yavari.kafka.operator.crd;

/**
 * User-defined listener on the broker. As of Wave 5 of the audit, listeners are
 * cluster-internal only — the Kroxylicious proxy is the sole external entry point for
 * Kafka traffic. Per-broker NodePort exposure was removed (no more
 * {@code externalAccess} / {@code nodePortBase} fields).
 */
public class KafkaListenerSpec {

    /** Listener name used verbatim in Kafka config (e.g. CLIENT_TLS). Uppercase, alphanumeric + underscores. */
    private String name;

    /** Port number. Must not conflict with 9092 (INTERNAL) or 9093 (CONTROLLER). */
    private int port;

    /** TLS configuration. null = plain listener. */
    private KafkaListenerTlsConfig tls;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public KafkaListenerTlsConfig getTls() { return tls; }
    public void setTls(KafkaListenerTlsConfig tls) { this.tls = tls; }
}
