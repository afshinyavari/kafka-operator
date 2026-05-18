package se.afshin.yavari.kafka.operator.crd;

public class KafkaListenerSpec {

    /** Listener name used verbatim in Kafka config (e.g. CLIENT_TLS). Uppercase, alphanumeric + underscores. */
    private String name;

    /** Port number. Must not conflict with 9092 (INTERNAL) or 9093 (CONTROLLER). */
    private int port;

    /** TLS configuration. null = plain listener. */
    private KafkaListenerTlsConfig tls;

    /** null = internal listener only. NODEPORT = create per-broker NodePort services. */
    private ExternalAccessType externalAccess;

    /** Base nodePort for NODEPORT type. Pod at ordinal N gets nodePortBase+N. Default 31000. */
    private int nodePortBase = 31000;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public KafkaListenerTlsConfig getTls() { return tls; }
    public void setTls(KafkaListenerTlsConfig tls) { this.tls = tls; }

    public ExternalAccessType getExternalAccess() { return externalAccess; }
    public void setExternalAccess(ExternalAccessType externalAccess) { this.externalAccess = externalAccess; }

    public int getNodePortBase() { return nodePortBase; }
    public void setNodePortBase(int nodePortBase) { this.nodePortBase = nodePortBase; }
}
