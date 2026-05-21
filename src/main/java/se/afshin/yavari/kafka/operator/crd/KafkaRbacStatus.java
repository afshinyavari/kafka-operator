package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

public class KafkaRbacStatus {

    public enum Phase { RECONCILING, READY, FAILED }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;
    private String message;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
