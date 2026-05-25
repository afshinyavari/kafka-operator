package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class KafkaConnectorStatus {

    public enum Phase { Reconciling, Ready, Failed, Unknown, Paused, Stopped }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.Reconciling;

    private Long observedGeneration;

    private List<Condition> conditions = new ArrayList<>();

    @PrinterColumn(name = "Connector", format = "", priority = 1)
    private String connectorState;

    @PrinterColumn(name = "Worker", format = "", priority = 1)
    private String workerId;

    private List<KafkaConnectorTaskStatus> tasks = new ArrayList<>();

    @PrinterColumn(name = "Tasks", format = "", priority = 1)
    private String tasksRunning;

    private Integer tasksTotal;

    /** Cache of the last-successfully-PUT desired config hash. Used by
     *  ConnectorDriftDetector to skip routine reconciles when neither the spec nor the
     *  config-from Secret has changed. */
    private String observedConfigHash;

    private String lastReconcileTime;

    private String message;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }

    public String getConnectorState() { return connectorState; }
    public void setConnectorState(String connectorState) { this.connectorState = connectorState; }

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public List<KafkaConnectorTaskStatus> getTasks() { return tasks; }
    public void setTasks(List<KafkaConnectorTaskStatus> tasks) { this.tasks = tasks; }

    public String getTasksRunning() { return tasksRunning; }
    public void setTasksRunning(String tasksRunning) { this.tasksRunning = tasksRunning; }

    public Integer getTasksTotal() { return tasksTotal; }
    public void setTasksTotal(Integer tasksTotal) { this.tasksTotal = tasksTotal; }

    public String getObservedConfigHash() { return observedConfigHash; }
    public void setObservedConfigHash(String observedConfigHash) { this.observedConfigHash = observedConfigHash; }

    public String getLastReconcileTime() { return lastReconcileTime; }
    public void setLastReconcileTime(String lastReconcileTime) { this.lastReconcileTime = lastReconcileTime; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}
