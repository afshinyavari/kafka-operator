package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class KafkaConnectStatus {

    public enum Phase { RECONCILING, READY, FAILED, SKIPPED, PENDING }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;

    private String message;

    @PrinterColumn(name = "Ready", format = "", priority = 1)
    private Integer readyReplicas;

    private Long observedGeneration;

    @PrinterColumn(name = "Bootstrap", format = "", priority = 1)
    private String bootstrap;

    /** In-cluster REST URL of the Connect cluster, e.g.
     *  {@code http://my-connect-connect.kafka.svc.cluster.local:8083}. Consumed by sibling
     *  {@code KafkaConnector} reconcilers and kafka-editor's Connect passthrough. */
    @PrinterColumn(name = "REST", format = "", priority = 1)
    private String url;

    /** Surfaced for KafkaConnector reconcilers: TLS material for the REST endpoint.
     *  v1 is plaintext-in-cluster so this is always null; reserved for v2. */
    private String tlsSecretRef;

    /** Surfaced for KafkaConnector reconcilers: Basic-auth credentials for the REST
     *  endpoint. v1 plaintext, reserved for v2. */
    private String authSecretRef;

    /** Final composed {@code plugin.path} that the workers boot with. Surfaced for
     *  debugging plugin-delivery problems. */
    private String pluginPath;

    private List<Condition> conditions = new ArrayList<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer readyReplicas) { this.readyReplicas = readyReplicas; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getBootstrap() { return bootstrap; }
    public void setBootstrap(String bootstrap) { this.bootstrap = bootstrap; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }

    public String getAuthSecretRef() { return authSecretRef; }
    public void setAuthSecretRef(String authSecretRef) { this.authSecretRef = authSecretRef; }

    public String getPluginPath() { return pluginPath; }
    public void setPluginPath(String pluginPath) { this.pluginPath = pluginPath; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
}
