package se.afshin.yavari.kafka.operator.crd;

public class KafkaNodePoolStatus {

    public enum Phase { PENDING, RECONCILING, READY, FAILED }

    private Phase phase = Phase.PENDING;
    private String message;
    private int readyReplicas;
    private int desiredReplicas;
    private String lastError;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public int getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(int readyReplicas) { this.readyReplicas = readyReplicas; }

    public int getDesiredReplicas() { return desiredReplicas; }
    public void setDesiredReplicas(int desiredReplicas) { this.desiredReplicas = desiredReplicas; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
}
