package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class MirrorMaker2Status {

    public enum Phase { RECONCILING, READY, FAILED, SKIPPED, PENDING }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;

    private String message;

    @PrinterColumn(name = "Ready", format = "", priority = 1)
    private Integer readyReplicas;

    private Long observedGeneration;

    @PrinterColumn(name = "Source", format = "", priority = 1)
    private String sourceBootstrap;

    @PrinterColumn(name = "Target", format = "", priority = 1)
    private String targetBootstrap;

    /** Per-connector status, parsed from the mm2-status.{flow} topic. Populated by
     *  Mm2StatusReader once the worker(s) come up and emit status records. */
    private List<ConnectorStatus> connectors = new ArrayList<>();

    private List<Condition> conditions = new ArrayList<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer readyReplicas) { this.readyReplicas = readyReplicas; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getSourceBootstrap() { return sourceBootstrap; }
    public void setSourceBootstrap(String sourceBootstrap) { this.sourceBootstrap = sourceBootstrap; }

    public String getTargetBootstrap() { return targetBootstrap; }
    public void setTargetBootstrap(String targetBootstrap) { this.targetBootstrap = targetBootstrap; }

    public List<ConnectorStatus> getConnectors() { return connectors; }
    public void setConnectors(List<ConnectorStatus> connectors) { this.connectors = connectors; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }

    public static class ConnectorStatus {
        public enum State { RUNNING, FAILED, PAUSED, UNASSIGNED, UNKNOWN }

        private String name;
        private State state = State.UNKNOWN;
        private Integer tasksRunning;
        private Integer tasksTotal;
        private String lastTransition;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public State getState() { return state; }
        public void setState(State state) { this.state = state; }

        public Integer getTasksRunning() { return tasksRunning; }
        public void setTasksRunning(Integer tasksRunning) { this.tasksRunning = tasksRunning; }

        public Integer getTasksTotal() { return tasksTotal; }
        public void setTasksTotal(Integer tasksTotal) { this.tasksTotal = tasksTotal; }

        public String getLastTransition() { return lastTransition; }
        public void setLastTransition(String lastTransition) { this.lastTransition = lastTransition; }
    }
}
