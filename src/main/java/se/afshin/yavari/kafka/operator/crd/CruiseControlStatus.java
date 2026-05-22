package se.afshin.yavari.kafka.operator.crd;

/**
 * Cruise Control sub-status on {@link KafkaClusterStatus}. Null when {@code spec.cruiseControl}
 * is unset. {@code SKIPPED} is set on every cluster except the primary one — Cruise Control
 * is a singleton that runs only on {@code spec.clusters[0]}.
 */
public class CruiseControlStatus {

    public enum Phase { RECONCILING, READY, FAILED, SKIPPED }

    private Phase phase = Phase.RECONCILING;
    private String message;

    /** In-cluster REST URL of the Cruise Control webserver, set once READY. */
    private String url;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
}
