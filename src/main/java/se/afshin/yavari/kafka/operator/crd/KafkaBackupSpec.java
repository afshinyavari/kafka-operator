package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.LinkedHashMap;
import java.util.Map;

/** Spec for a {@link KafkaBackup} — a scheduled, recurring topic-data backup. */
public class KafkaBackupSpec {

    /** kafka-backup tool image (osodevops/kafka-backup compiled on UBI). */
    private String image = "kafka-backup:dev";

    private String imagePullPolicy = "IfNotPresent";

    /** Managed KafkaCluster to back up. */
    @Required
    private KafkaClusterRef clusterRef;

    /** Cluster to run the backup CronJob on. Required when the operator runs
     *  multi-cluster — enforced by the reconciler, not the CRD schema. */
    private BackupPlacement placement;

    private BackupTopicSelector topics = new BackupTopicSelector();

    @Required
    private BackupStorageSpec storage;

    private BackupCompression compression = BackupCompression.ZSTD;

    /** Optional codec level. Null = tool default. */
    private Integer compressionLevel;

    /** Cron schedule for the backup CronJob. */
    @Required
    @ValidationRule(value = "self.size() > 0", message = "schedule must not be blank")
    private String schedule;

    /** CronJob concurrency policy. */
    @ValidationRule(value = "self in ['Allow', 'Forbid', 'Replace']",
            message = "concurrencyPolicy must be Allow, Forbid or Replace")
    private String concurrencyPolicy = "Forbid";

    /** Pause the schedule without deleting the CR. */
    private boolean suspend = false;

    private Long startingDeadlineSeconds = 300L;
    private Integer successfulJobsHistoryLimit = 3;
    private Integer failedJobsHistoryLimit = 3;

    /** Also export Apicurio schemas with each backup. Null = auto (true when the
     *  referenced cluster has an apicurio sub-spec). */
    private Boolean includeSchemas;

    /** Secret with OAuth2 client-credentials ({@code token-url}, {@code client-id},
     *  {@code client-secret}) for the Apicurio schema export. Optional. */
    private String schemaRegistryAuthSecretRef;

    /** Per-run hard timeout for the backup Job. */
    private Long activeDeadlineSeconds = 3600L;

    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();

    /** Extra keys merged verbatim into the rendered {@code backup:} config section. */
    private Map<String, String> additionalConfig = new LinkedHashMap<>();

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public KafkaClusterRef getClusterRef() { return clusterRef; }
    public void setClusterRef(KafkaClusterRef clusterRef) { this.clusterRef = clusterRef; }

    public BackupPlacement getPlacement() { return placement; }
    public void setPlacement(BackupPlacement placement) { this.placement = placement; }

    public BackupTopicSelector getTopics() { return topics; }
    public void setTopics(BackupTopicSelector topics) { this.topics = topics; }

    public BackupStorageSpec getStorage() { return storage; }
    public void setStorage(BackupStorageSpec storage) { this.storage = storage; }

    public BackupCompression getCompression() { return compression; }
    public void setCompression(BackupCompression compression) { this.compression = compression; }

    public Integer getCompressionLevel() { return compressionLevel; }
    public void setCompressionLevel(Integer compressionLevel) { this.compressionLevel = compressionLevel; }

    public String getSchedule() { return schedule; }
    public void setSchedule(String schedule) { this.schedule = schedule; }

    public String getConcurrencyPolicy() { return concurrencyPolicy; }
    public void setConcurrencyPolicy(String concurrencyPolicy) { this.concurrencyPolicy = concurrencyPolicy; }

    public boolean isSuspend() { return suspend; }
    public void setSuspend(boolean suspend) { this.suspend = suspend; }

    public Long getStartingDeadlineSeconds() { return startingDeadlineSeconds; }
    public void setStartingDeadlineSeconds(Long startingDeadlineSeconds) {
        this.startingDeadlineSeconds = startingDeadlineSeconds;
    }

    public Integer getSuccessfulJobsHistoryLimit() { return successfulJobsHistoryLimit; }
    public void setSuccessfulJobsHistoryLimit(Integer successfulJobsHistoryLimit) {
        this.successfulJobsHistoryLimit = successfulJobsHistoryLimit;
    }

    public Integer getFailedJobsHistoryLimit() { return failedJobsHistoryLimit; }
    public void setFailedJobsHistoryLimit(Integer failedJobsHistoryLimit) {
        this.failedJobsHistoryLimit = failedJobsHistoryLimit;
    }

    public Boolean getIncludeSchemas() { return includeSchemas; }
    public void setIncludeSchemas(Boolean includeSchemas) { this.includeSchemas = includeSchemas; }

    public String getSchemaRegistryAuthSecretRef() { return schemaRegistryAuthSecretRef; }
    public void setSchemaRegistryAuthSecretRef(String schemaRegistryAuthSecretRef) {
        this.schemaRegistryAuthSecretRef = schemaRegistryAuthSecretRef;
    }

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
