package se.afshin.yavari.kafka.operator.crd;

import java.util.Map;

public class KafkaUIIngressConfig {
    private boolean enabled = false;
    private String className;
    private String host;
    private String tlsSecret;
    private Map<String, String> annotations = Map.of();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public String getTlsSecret() { return tlsSecret; }
    public void setTlsSecret(String tlsSecret) { this.tlsSecret = tlsSecret; }

    public Map<String, String> getAnnotations() { return annotations; }
    public void setAnnotations(Map<String, String> annotations) { this.annotations = annotations; }
}
