package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class KafkaRestoreStatus {

    public enum Phase { PENDING, RUNNING, SUCCEEDED, FAILED, SKIPPED }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.PENDING;

    private String message;
    private Long observedGeneration;

    @PrinterColumn(name = "Job", format = "", priority = 1)
    private String jobName;

    private String resolvedBootstrap;
    private String startTime;

    @PrinterColumn(name = "Completed", format = "", priority = 1)
    private String completionTime;

    private List<Condition> conditions = new ArrayList<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }

    public String getResolvedBootstrap() { return resolvedBootstrap; }
    public void setResolvedBootstrap(String resolvedBootstrap) { this.resolvedBootstrap = resolvedBootstrap; }

    public String getStartTime() { return startTime; }
    public void setStartTime(String startTime) { this.startTime = startTime; }

    public String getCompletionTime() { return completionTime; }
    public void setCompletionTime(String completionTime) { this.completionTime = completionTime; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
}
