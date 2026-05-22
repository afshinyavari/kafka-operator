package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Observed state of a {@link KafkaRebalance}, driven by the annotation-based state machine:
 * {@code NEW → PENDING_PROPOSAL → PROPOSAL_READY →(approve)→ REBALANCING → READY}, plus
 * {@code STOPPED} and {@code NOT_READY}.
 */
public class KafkaRebalanceStatus {

    public enum Phase { NEW, PENDING_PROPOSAL, PROPOSAL_READY, REBALANCING, READY, STOPPED, NOT_READY }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.NEW;

    private String message;
    private String lastReconcileTime;
    private Long observedGeneration;

    /** Cruise Control User-Task-ID of the in-flight proposal (dryrun) request. */
    private String sessionId;

    /** Cruise Control User-Task-ID of the in-flight execution (dryrun=false) request. */
    private String executionTaskId;

    /** Flattened Cruise Control optimization-proposal summary (data to move, # movements,
     *  balancedness scores, …). Populated when the proposal becomes ready. */
    private Map<String, String> optimizationResult = new LinkedHashMap<>();

    /** Denormalized {@code dataToMoveMB} from {@link #optimizationResult}, surfaced as a
     *  printer column for {@code kubectl get kafkarebalance}. */
    @PrinterColumn(name = "DataToMoveMB", format = "", priority = 1)
    private String dataToMoveMB;

    private List<Condition> conditions = new ArrayList<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getLastReconcileTime() { return lastReconcileTime; }
    public void setLastReconcileTime(String lastReconcileTime) { this.lastReconcileTime = lastReconcileTime; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getExecutionTaskId() { return executionTaskId; }
    public void setExecutionTaskId(String executionTaskId) { this.executionTaskId = executionTaskId; }

    public Map<String, String> getOptimizationResult() { return optimizationResult; }
    public void setOptimizationResult(Map<String, String> optimizationResult) {
        this.optimizationResult = optimizationResult;
    }

    public String getDataToMoveMB() { return dataToMoveMB; }
    public void setDataToMoveMB(String dataToMoveMB) { this.dataToMoveMB = dataToMoveMB; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
}
