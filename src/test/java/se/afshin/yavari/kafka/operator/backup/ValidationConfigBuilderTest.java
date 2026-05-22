package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidation;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidationSpec;
import se.afshin.yavari.kafka.operator.crd.PvcStorageConfig;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import static org.assertj.core.api.Assertions.assertThat;

class ValidationConfigBuilderTest {

    private final ValidationConfigBuilder builder = new ValidationConfigBuilder();

    private BackupStorageSpec s3(String bucket, String prefix) {
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket(bucket);
        s3.setPrefix(prefix);
        s3.setCredentialsSecretRef("s3-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        return storage;
    }

    private BackupStorageSpec pvc() {
        PvcStorageConfig pvc = new PvcStorageConfig();
        pvc.setClaimName("backups");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setPvc(pvc);
        return storage;
    }

    private KafkaBackupValidation cr(boolean deep) {
        KafkaBackupValidation cr = new KafkaBackupValidation();
        cr.setMetadata(new ObjectMetaBuilder().withName("v1").withNamespace("kafka").build());
        KafkaBackupValidationSpec spec = new KafkaBackupValidationSpec();
        spec.setDeep(deep);
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void storagePathForS3IncludesBucketAndPrefix() {
        assertThat(ValidationConfigBuilder.storagePath(s3("my-bucket", "prod/")))
                .isEqualTo("s3://my-bucket/prod");
    }

    @Test
    void storagePathForS3OmitsEmptyPrefix() {
        assertThat(ValidationConfigBuilder.storagePath(s3("my-bucket", "")))
                .isEqualTo("s3://my-bucket");
    }

    @Test
    void storagePathForPvcIsMountPath() {
        assertThat(ValidationConfigBuilder.storagePath(pvc())).isEqualTo("/backup");
    }

    @Test
    void validateArgsIncludeDeepFlagWhenRequested() {
        assertThat(builder.validateArgs(cr(true), s3("b", "p"), "backup-7"))
                .containsExactly("validate", "--path", "s3://b/p",
                        "--backup-id", "backup-7", "--deep");
    }

    @Test
    void validateArgsOmitDeepFlagWhenDisabled() {
        assertThat(builder.validateArgs(cr(false), pvc(), "backup-7"))
                .containsExactly("validate", "--path", "/backup", "--backup-id", "backup-7");
    }
}
