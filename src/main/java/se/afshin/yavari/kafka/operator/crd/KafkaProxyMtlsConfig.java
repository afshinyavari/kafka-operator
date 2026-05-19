package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyMtlsConfig {

    private boolean enabled;
    private String proxyPrincipal = "kafka-proxy";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getProxyPrincipal() { return proxyPrincipal; }
    public void setProxyPrincipal(String proxyPrincipal) { this.proxyPrincipal = proxyPrincipal; }
}
