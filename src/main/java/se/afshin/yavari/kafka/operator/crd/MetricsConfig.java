package se.afshin.yavari.kafka.operator.crd;

public class MetricsConfig {

    /** Name of a ConfigMap in the same namespace containing a jmx-config.yaml key. */
    private String configMapRef;

    public String getConfigMapRef() { return configMapRef; }
    public void setConfigMapRef(String configMapRef) { this.configMapRef = configMapRef; }
}
