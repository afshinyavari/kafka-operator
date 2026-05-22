package se.afshin.yavari.kafka.operator.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.generator.annotation.ValidationRule;

/** Where a restore/validation reads its backup from: either a {@link KafkaBackup} CR
 *  in the same namespace (reuse its storage config) or an inline storage block. */
@ValidationRule(
        value = "(has(self.kafkaBackupRef) && !has(self.storage)) "
                + "|| (!has(self.kafkaBackupRef) && has(self.storage))",
        message = "exactly one of source.kafkaBackupRef or source.storage must be set"
)
public class RestoreSourceSpec {

    /** Name of a KafkaBackup CR in the same namespace. */
    private String kafkaBackupRef;

    /** Inline storage location of the backup. */
    private BackupStorageSpec storage;

    public String getKafkaBackupRef() { return kafkaBackupRef; }
    public void setKafkaBackupRef(String kafkaBackupRef) { this.kafkaBackupRef = kafkaBackupRef; }

    public BackupStorageSpec getStorage() { return storage; }
    public void setStorage(BackupStorageSpec storage) { this.storage = storage; }

    @JsonIgnore
    public boolean hasBackupRef() { return kafkaBackupRef != null && !kafkaBackupRef.isBlank(); }

    @JsonIgnore
    public boolean hasInlineStorage() { return storage != null; }
}
