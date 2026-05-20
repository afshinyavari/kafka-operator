package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyStatus {

    public enum Phase { RECONCILING, READY, FAILED, SKIPPED }

    private Phase phase = Phase.RECONCILING;
    private String message;
    private Integer readyReplicas;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer readyReplicas) { this.readyReplicas = readyReplicas; }
}
