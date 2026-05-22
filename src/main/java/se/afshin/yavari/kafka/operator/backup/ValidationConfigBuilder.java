package se.afshin.yavari.kafka.operator.backup;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidation;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the {@code kafka-backup validate} command for a {@link KafkaBackupValidation}.
 * Unlike backup/restore, validation takes no config file — it reads a storage {@code --path}
 * plus a {@code --backup-id} directly. Cloud storage is addressed by object-store URL.
 */
@ApplicationScoped
public class ValidationConfigBuilder {

    /** Full argv for the validate container. */
    public List<String> validateArgs(KafkaBackupValidation cr, BackupStorageSpec storage,
                                     String backupId) {
        List<String> args = new ArrayList<>();
        args.add("validate");
        args.add("--path");
        args.add(storagePath(storage));
        args.add("--backup-id");
        args.add(backupId);
        if (cr.getSpec().isDeep()) {
            args.add("--deep");
        }
        return args;
    }

    /** Storage location in the form the inspection sub-commands accept: an absolute path
     *  for PVC storage, an object-store URL for cloud backends. */
    public static String storagePath(BackupStorageSpec storage) {
        return switch (storage.resolveType()) {
            case PVC -> BackupPaths.PVC_DIR;
            case S3 -> {
                S3StorageConfig s3 = storage.getS3();
                yield "s3://" + s3.getBucket() + suffix(s3.getPrefix());
            }
            case AZURE -> "azure://" + storage.getAzure().getContainer()
                    + suffix(storage.getAzure().getPrefix());
            case GCS -> "gcs://" + storage.getGcs().getBucket()
                    + suffix(storage.getGcs().getPrefix());
        };
    }

    private static String suffix(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return "";
        }
        String trimmed = prefix;
        while (trimmed.startsWith("/")) trimmed = trimmed.substring(1);
        while (trimmed.endsWith("/")) trimmed = trimmed.substring(0, trimmed.length() - 1);
        return trimmed.isEmpty() ? "" : "/" + trimmed;
    }
}
