package se.afshin.yavari.kafka.operator.backup;

import java.util.LinkedHashMap;
import java.util.Map;

/** Standard label set for all resources owned by a KafkaBackup / KafkaRestore /
 *  KafkaBackupValidation CR. */
public final class BackupLabels {

    private BackupLabels() {}

    /** @param component one of {@code kafka-backup}, {@code kafka-restore},
     *                   {@code kafka-backup-validation}
     *  @param instance  the CR's {@code metadata.name} */
    public static Map<String, String> labels(String component, String instance) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("app", component);
        m.put("app.instance", instance);
        m.put("app.managed-by", "kafka-operator");
        return m;
    }
}
