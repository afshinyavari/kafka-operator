package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

public class KafkaNodePoolStatus {

    public enum Phase { PENDING, RECONCILING, READY, FAILED }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.PENDING;
    private String message;
    @PrinterColumn(name = "Ready", format = "", priority = 0)
    private int readyReplicas;
    @PrinterColumn(name = "Desired", format = "", priority = 0)
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
