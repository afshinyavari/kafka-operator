package se.afshin.yavari.kafka.operator.crd;

public class KafkaRbacStatus {

    public enum Phase { RECONCILING, READY, FAILED }

    private Phase phase = Phase.RECONCILING;
    private String message;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
