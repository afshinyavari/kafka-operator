package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Desired state of a {@link KafkaRebalance}. {@code brokers} is required for the
 * {@code ADD_BROKERS} / {@code REMOVE_BROKERS} modes — that is enforced at reconcile time
 * (a clear status message beats a CEL cost-budget rejection).
 */
public class KafkaRebalanceSpec {

    /** Name of the target {@link KafkaCluster} in the same namespace. The cluster must
     *  have {@code spec.cruiseControl} set (Cruise Control deployed). */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "clusterRef must not be blank")
    private String clusterRef;

    /** Rebalance kind. Defaults to a full cluster rebalance. */
    private KafkaRebalanceMode mode = KafkaRebalanceMode.FULL;

    /** Broker IDs to add or remove. Required for ADD_BROKERS / REMOVE_BROKERS; ignored for FULL. */
    private List<Integer> brokers = new ArrayList<>();

    /** Optional Cruise Control goal class names. Empty → Cruise Control's configured goals. */
    private List<String> goals = new ArrayList<>();

    /** Skip the check that the proposal satisfies all hard goals. */
    private boolean skipHardGoalCheck = false;

    /** Rebalance disk usage between log dirs on each broker (intra-broker), not across brokers. */
    private boolean rebalanceDisk = false;

    /** Cap on concurrent partition movements per broker. Null → Cruise Control default. */
    private Integer concurrentPartitionMovementsPerBroker;

    /** Cap on concurrent leadership movements. Null → Cruise Control default. */
    private Integer concurrentLeaderMovements;

    /** Replication throttle in bytes/sec applied during execution. Null → unthrottled. */
    private Long replicationThrottle;

    /** Regex of topic names to exclude from replica movement. Null → none excluded. */
    private String excludedTopics;

    public String getClusterRef() { return clusterRef; }
    public void setClusterRef(String clusterRef) { this.clusterRef = clusterRef; }

    public KafkaRebalanceMode getMode() { return mode; }
    public void setMode(KafkaRebalanceMode mode) { this.mode = mode; }

    public List<Integer> getBrokers() { return brokers; }
    public void setBrokers(List<Integer> brokers) { this.brokers = brokers; }

    public List<String> getGoals() { return goals; }
    public void setGoals(List<String> goals) { this.goals = goals; }

    public boolean isSkipHardGoalCheck() { return skipHardGoalCheck; }
    public void setSkipHardGoalCheck(boolean skipHardGoalCheck) { this.skipHardGoalCheck = skipHardGoalCheck; }

    public boolean isRebalanceDisk() { return rebalanceDisk; }
    public void setRebalanceDisk(boolean rebalanceDisk) { this.rebalanceDisk = rebalanceDisk; }

    public Integer getConcurrentPartitionMovementsPerBroker() { return concurrentPartitionMovementsPerBroker; }
    public void setConcurrentPartitionMovementsPerBroker(Integer v) { this.concurrentPartitionMovementsPerBroker = v; }

    public Integer getConcurrentLeaderMovements() { return concurrentLeaderMovements; }
    public void setConcurrentLeaderMovements(Integer v) { this.concurrentLeaderMovements = v; }

    public Long getReplicationThrottle() { return replicationThrottle; }
    public void setReplicationThrottle(Long replicationThrottle) { this.replicationThrottle = replicationThrottle; }

    public String getExcludedTopics() { return excludedTopics; }
    public void setExcludedTopics(String excludedTopics) { this.excludedTopics = excludedTopics; }
}
