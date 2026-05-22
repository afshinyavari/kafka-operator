package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;

/** Wraps a rendered kafka-backup YAML config in an owner-ref'd ConfigMap. The config
 *  never contains storage credentials — those are mounted as Secret-backed env vars. */
@ApplicationScoped
public class BackupConfigMapBuilder {

    public static final String BACKUP_KEY = "backup.yaml";
    public static final String RESTORE_KEY = "restore.yaml";

    public ConfigMap build(HasMetadata cr, String dataKey, String renderedYaml,
                           Map<String, String> labels, OwnerReference ownerRef) {
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(cr.getMetadata().getName())
                    .withNamespace(cr.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withData(Map.of(dataKey, renderedYaml))
                .build();
    }
}
