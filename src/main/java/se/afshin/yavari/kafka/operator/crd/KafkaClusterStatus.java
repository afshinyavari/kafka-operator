package se.afshin.yavari.kafka.operator.crd;

import java.util.LinkedHashMap;
import java.util.Map;

public class KafkaClusterStatus {

    public enum Phase { RECONCILING, READY, DEGRADED, FAILED }

    private Phase phase = Phase.RECONCILING;
    private String message;
    private String lastReconcileTime;
    private Long observedGeneration;

    /** Per-pool readiness summary, keyed by pool name. */
    private Map<String, String> poolPhases = new LinkedHashMap<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getLastReconcileTime() { return lastReconcileTime; }
    public void setLastReconcileTime(String lastReconcileTime) { this.lastReconcileTime = lastReconcileTime; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public Map<String, String> getPoolPhases() { return poolPhases; }
    public void setPoolPhases(Map<String, String> poolPhases) { this.poolPhases = poolPhases; }
}
