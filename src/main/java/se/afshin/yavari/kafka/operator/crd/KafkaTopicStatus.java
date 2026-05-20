package se.afshin.yavari.kafka.operator.crd;

public class KafkaTopicStatus {

    public enum Phase { RECONCILING, READY, SKIPPED, FAILED }

    private Phase phase = Phase.RECONCILING;
    private String message;
    private Long observedGeneration;
    private String topicId;
    private Integer observedPartitions;
    private Short observedReplicationFactor;
    private String lastReconcileTime;

    public Phase getPhase() { return phase; }
    public void setPhase(Phase phase) { this.phase = phase; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long observedGeneration) { this.observedGeneration = observedGeneration; }

    public String getTopicId() { return topicId; }
    public void setTopicId(String topicId) { this.topicId = topicId; }

    public Integer getObservedPartitions() { return observedPartitions; }
    public void setObservedPartitions(Integer observedPartitions) { this.observedPartitions = observedPartitions; }

    public Short getObservedReplicationFactor() { return observedReplicationFactor; }
    public void setObservedReplicationFactor(Short observedReplicationFactor) { this.observedReplicationFactor = observedReplicationFactor; }

    public String getLastReconcileTime() { return lastReconcileTime; }
    public void setLastReconcileTime(String lastReconcileTime) { this.lastReconcileTime = lastReconcileTime; }
}
