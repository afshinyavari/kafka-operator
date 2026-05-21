package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** MM2 replication-flow tuning. All fields have sensible defaults; tighten them per flow. */
public class Mm2FlowConfig {

    /** Logical flow name. Used as the suffix on internal topics
     *  (mm2-configs.{flow}, mm2-offsets.{flow}, mm2-status.{flow}). Defaults to
     *  the CR's metadata.name when unset. Two MM2 CRs that share a flowName will
     *  collide on internal topics — keep them distinct. */
    @ValidationRule(value = "self.matches('^[a-zA-Z0-9._-]{1,128}$')",
            message = "flowName must match ^[a-zA-Z0-9._-]{1,128}$")
    private String flowName;

    /** Allowlist regexes for topics to mirror. Default: everything except internals. */
    private List<String> topics = List.of(".*");

    /** Denylist regexes evaluated after topics. */
    private List<String> topicsExclude = List.of();

    /** Consumer group allowlist regexes (for checkpoint emission). */
    private List<String> groups = List.of(".*");

    /** Consumer group denylist regexes. */
    private List<String> groupsExclude = List.of();

    /** Target-side replication factor for mirrored topics and the 3 internal topics. */
    @ValidationRule(value = "self >= 1", message = "replicationFactor must be at least 1")
    private int replicationFactor = 3;

    private boolean syncTopicAcls = false;
    private boolean syncTopicConfigs = true;
    private boolean emitHeartbeats = true;

    @ValidationRule(value = "self >= 1", message = "tasksMax must be at least 1")
    private int tasksMax = 4;

    /** Escape hatch for raw connector properties. Keys are passed through verbatim to
     *  the MM2 properties file under the appropriate connector prefix. */
    private Map<String, String> additionalProperties = new HashMap<>();

    public String getFlowName() { return flowName; }
    public void setFlowName(String flowName) { this.flowName = flowName; }

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }

    public List<String> getTopicsExclude() { return topicsExclude; }
    public void setTopicsExclude(List<String> topicsExclude) { this.topicsExclude = topicsExclude; }

    public List<String> getGroups() { return groups; }
    public void setGroups(List<String> groups) { this.groups = groups; }

    public List<String> getGroupsExclude() { return groupsExclude; }
    public void setGroupsExclude(List<String> groupsExclude) { this.groupsExclude = groupsExclude; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public boolean isSyncTopicAcls() { return syncTopicAcls; }
    public void setSyncTopicAcls(boolean syncTopicAcls) { this.syncTopicAcls = syncTopicAcls; }

    public boolean isSyncTopicConfigs() { return syncTopicConfigs; }
    public void setSyncTopicConfigs(boolean syncTopicConfigs) { this.syncTopicConfigs = syncTopicConfigs; }

    public boolean isEmitHeartbeats() { return emitHeartbeats; }
    public void setEmitHeartbeats(boolean emitHeartbeats) { this.emitHeartbeats = emitHeartbeats; }

    public int getTasksMax() { return tasksMax; }
    public void setTasksMax(int tasksMax) { this.tasksMax = tasksMax; }

    public Map<String, String> getAdditionalProperties() { return additionalProperties; }
    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }
}
