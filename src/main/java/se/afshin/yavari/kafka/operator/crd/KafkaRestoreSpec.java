package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Spec for a {@link KafkaRestore} — a one-shot restore into a managed cluster. */
public class KafkaRestoreSpec {

    private String image = "kafka-backup:dev";
    private String imagePullPolicy = "IfNotPresent";

    /** Managed KafkaCluster to restore into. */
    @Required
    private KafkaClusterRef targetClusterRef;

    private BackupPlacement placement;

    /** Where the backup is read from. */
    @Required
    private RestoreSourceSpec source;

    /** Specific backup id (default: the kafkaBackupRef name, or latest). */
    private String backupId;

    private BackupTopicSelector topics = new BackupTopicSelector();

    /** Optional source-topic to target-topic renaming. Use this to restore into
     *  non-live topics. Maps to the osodevops {@code restore.topic_mapping}. */
    private Map<String, String> topicMapping = new LinkedHashMap<>();

    /** Optional point-in-time-recovery window. */
    private RestoreTimeWindow timeWindow;

    /** Restore committed consumer-group offsets. Dangerous against live groups —
     *  the reconciler rejects the restore if any listed group has active members. */
    private boolean restoreOffsets = false;

    /** Consumer groups whose offsets to restore (when restoreOffsets=true). */
    private List<String> consumerGroups = new ArrayList<>();

    /** Pre-flight guard on target topics. */
    private RestoreTargetPolicy targetPolicy = RestoreTargetPolicy.REQUIRE_EMPTY;

    /** Must be true — restore is destructive; the reconciler refuses otherwise. */
    private boolean confirm = false;

    /** Import Apicurio schemas before records. Null = auto (true when the backup
     *  was taken with includeSchemas and the target cluster has apicurio). */
    private Boolean restoreSchemas;

    private String schemaRegistryAuthSecretRef;

    /** Create target topics that don't exist. Maps to {@code restore.create_topics}. */
    private boolean createTopics = true;

    /** Dry-run: report what would be restored without writing. */
    private boolean dryRun = false;

    private Long activeDeadlineSeconds = 7200L;

    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();

    /** Extra keys merged verbatim into the rendered {@code restore:} config section. */
    private Map<String, String> additionalConfig = new LinkedHashMap<>();

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public KafkaClusterRef getTargetClusterRef() { return targetClusterRef; }
    public void setTargetClusterRef(KafkaClusterRef targetClusterRef) {
        this.targetClusterRef = targetClusterRef;
    }

    public BackupPlacement getPlacement() { return placement; }
    public void setPlacement(BackupPlacement placement) { this.placement = placement; }

    public RestoreSourceSpec getSource() { return source; }
    public void setSource(RestoreSourceSpec source) { this.source = source; }

    public String getBackupId() { return backupId; }
    public void setBackupId(String backupId) { this.backupId = backupId; }

    public BackupTopicSelector getTopics() { return topics; }
    public void setTopics(BackupTopicSelector topics) { this.topics = topics; }

    public Map<String, String> getTopicMapping() { return topicMapping; }
    public void setTopicMapping(Map<String, String> topicMapping) { this.topicMapping = topicMapping; }

    public RestoreTimeWindow getTimeWindow() { return timeWindow; }
    public void setTimeWindow(RestoreTimeWindow timeWindow) { this.timeWindow = timeWindow; }

    public boolean isRestoreOffsets() { return restoreOffsets; }
    public void setRestoreOffsets(boolean restoreOffsets) { this.restoreOffsets = restoreOffsets; }

    public List<String> getConsumerGroups() { return consumerGroups; }
    public void setConsumerGroups(List<String> consumerGroups) { this.consumerGroups = consumerGroups; }

    public RestoreTargetPolicy getTargetPolicy() { return targetPolicy; }
    public void setTargetPolicy(RestoreTargetPolicy targetPolicy) { this.targetPolicy = targetPolicy; }

    public boolean isConfirm() { return confirm; }
    public void setConfirm(boolean confirm) { this.confirm = confirm; }

    public Boolean getRestoreSchemas() { return restoreSchemas; }
    public void setRestoreSchemas(Boolean restoreSchemas) { this.restoreSchemas = restoreSchemas; }

    public String getSchemaRegistryAuthSecretRef() { return schemaRegistryAuthSecretRef; }
    public void setSchemaRegistryAuthSecretRef(String schemaRegistryAuthSecretRef) {
        this.schemaRegistryAuthSecretRef = schemaRegistryAuthSecretRef;
    }

    public boolean isCreateTopics() { return createTopics; }
    public void setCreateTopics(boolean createTopics) { this.createTopics = createTopics; }

    public boolean isDryRun() { return dryRun; }
    public void setDryRun(boolean dryRun) { this.dryRun = dryRun; }

    public Long getActiveDeadlineSeconds() { return activeDeadlineSeconds; }
    public void setActiveDeadlineSeconds(Long activeDeadlineSeconds) {
        this.activeDeadlineSeconds = activeDeadlineSeconds;
    }

    public KafkaUIResourceRequirements getResources() { return resources; }
    public void setResources(KafkaUIResourceRequirements resources) { this.resources = resources; }

    public Map<String, String> getAdditionalConfig() { return additionalConfig; }
    public void setAdditionalConfig(Map<String, String> additionalConfig) {
        this.additionalConfig = additionalConfig;
    }
}
