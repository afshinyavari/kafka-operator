package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

public class KafkaUIStatus {

    public enum Phase { RECONCILING, READY, FAILED, SKIPPED }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;
    private String message;
    @PrinterColumn(name = "Ready", format = "", priority = 1)
    private Integer readyReplicas;
    private Long observedGeneration;
    @PrinterColumn(name = "Host", format = "", priority = 0)
    private String advertisedHost;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer readyReplicas) { this.readyReplicas = readyReplicas; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getAdvertisedHost() { return advertisedHost; }
    public void setAdvertisedHost(String advertisedHost) { this.advertisedHost = advertisedHost; }
}
