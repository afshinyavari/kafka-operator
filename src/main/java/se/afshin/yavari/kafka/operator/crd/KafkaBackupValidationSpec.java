package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

import java.util.LinkedHashMap;
import java.util.Map;

/** Spec for a {@link KafkaBackupValidation} — a one-shot backup integrity check. */
public class KafkaBackupValidationSpec {

    private String image = "kafka-backup:dev";
    private String imagePullPolicy = "IfNotPresent";

    private BackupPlacement placement;

    /** Where the backup to validate lives. */
    @Required
    private RestoreSourceSpec source;

    /** Specific backup id (default: the kafkaBackupRef name). */
    private String backupId;

    private ReportFormat reportFormat = ReportFormat.JSON;

    /** Deep validation reads all data and verifies checksums; quick checks only
     *  segment structure and metadata. */
    private boolean deep = true;

    private Long activeDeadlineSeconds = 3600L;

    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();

    private Map<String, String> additionalConfig = new LinkedHashMap<>();

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public BackupPlacement getPlacement() { return placement; }
    public void setPlacement(BackupPlacement placement) { this.placement = placement; }

    public RestoreSourceSpec getSource() { return source; }
    public void setSource(RestoreSourceSpec source) { this.source = source; }

    public String getBackupId() { return backupId; }
    public void setBackupId(String backupId) { this.backupId = backupId; }

    public ReportFormat getReportFormat() { return reportFormat; }
    public void setReportFormat(ReportFormat reportFormat) { this.reportFormat = reportFormat; }

    public boolean isDeep() { return deep; }
    public void setDeep(boolean deep) { this.deep = deep; }

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
