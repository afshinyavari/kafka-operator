package se.afshin.yavari.kafka.operator.crd;

public class ApicurioRegistryStatus {

    public enum Phase { RECONCILING, READY, FAILED }

    private Phase phase = Phase.RECONCILING;
    private String message;
    private String registryUrl;
    private String proxyUrl;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getRegistryUrl() { return registryUrl; }
    public void setRegistryUrl(String registryUrl) { this.registryUrl = registryUrl; }

    public String getProxyUrl() { return proxyUrl; }
    public void setProxyUrl(String proxyUrl) { this.proxyUrl = proxyUrl; }
}
