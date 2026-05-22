package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.crd.generator.annotation.PrinterColumn;

import java.util.ArrayList;
import java.util.List;

public class KafkaBackupStatus {

    public enum Phase { RECONCILING, SCHEDULED, SUSPENDED, SKIPPED, FAILED }

    public enum JobResult { SUCCEEDED, FAILED, RUNNING, UNKNOWN }

    @PrinterColumn(name = "Phase", format = "", priority = 0)
    private Phase phase = Phase.RECONCILING;

    private String message;
    private Long observedGeneration;

    @PrinterColumn(name = "Schedule", format = "", priority = 1)
    private String schedule;

    private String cronJobName;
    private String resolvedBootstrap;
    private String lastScheduleTime;

    @PrinterColumn(name = "LastBackup", format = "", priority = 0)
    private String lastSuccessfulBackupTime;

    private String lastJobName;

    @PrinterColumn(name = "LastResult", format = "", priority = 1)
    private JobResult lastJobResult;

    private Integer activeBackupCount;

    private List<Condition> conditions = new ArrayList<>();

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getSchedule() { return schedule; }
    public void setSchedule(String schedule) { this.schedule = schedule; }

    public String getCronJobName() { return cronJobName; }
    public void setCronJobName(String cronJobName) { this.cronJobName = cronJobName; }

    public String getResolvedBootstrap() { return resolvedBootstrap; }
    public void setResolvedBootstrap(String resolvedBootstrap) { this.resolvedBootstrap = resolvedBootstrap; }

    public String getLastScheduleTime() { return lastScheduleTime; }
    public void setLastScheduleTime(String lastScheduleTime) { this.lastScheduleTime = lastScheduleTime; }

    public String getLastSuccessfulBackupTime() { return lastSuccessfulBackupTime; }
    public void setLastSuccessfulBackupTime(String lastSuccessfulBackupTime) {
        this.lastSuccessfulBackupTime = lastSuccessfulBackupTime;
    }

    public String getLastJobName() { return lastJobName; }
    public void setLastJobName(String lastJobName) { this.lastJobName = lastJobName; }

    public JobResult getLastJobResult() { return lastJobResult; }
    public void setLastJobResult(JobResult lastJobResult) { this.lastJobResult = lastJobResult; }

    public Integer getActiveBackupCount() { return activeBackupCount; }
    public void setActiveBackupCount(Integer activeBackupCount) { this.activeBackupCount = activeBackupCount; }

    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> conditions) { this.conditions = conditions; }
}
