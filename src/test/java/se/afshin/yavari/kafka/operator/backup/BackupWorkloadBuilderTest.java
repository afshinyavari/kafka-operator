package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackup;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRestore;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidation;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidationSpec;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BackupWorkloadBuilderTest {

    private BackupWorkloadBuilder builder;
    private final OwnerReference ownerRef = new OwnerReferenceBuilder().withName("owner").build();

    @BeforeEach
    void setUp() {
        builder = new BackupWorkloadBuilder();
        builder.storageMounts = new BackupStorageMounts();
        builder.schemaBackupStep = new SchemaBackupStep();
    }

    private BackupStorageSpec s3() {
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket("bk");
        s3.setCredentialsSecretRef("s3-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        return storage;
    }

    private KafkaBackup backupCr() {
        KafkaBackup cr = new KafkaBackup();
        cr.setMetadata(new ObjectMetaBuilder().withName("b1").withNamespace("kafka").build());
        KafkaBackupSpec spec = new KafkaBackupSpec();
        spec.setSchedule("0 2 * * *");
        spec.setStorage(s3());
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void backupCronJobCarriesScheduleConcurrencyAndConfigHash() {
        CronJob cj = builder.buildBackupCronJob(backupCr(),
                new ResolvedBackupEndpoint("broker:9092", null, null), "hash123", false, ownerRef);

        assertThat(cj.getSpec().getSchedule()).isEqualTo("0 2 * * *");
        assertThat(cj.getSpec().getConcurrencyPolicy()).isEqualTo("Forbid");
        assertThat(cj.getSpec().getSuspend()).isFalse();
        var pod = cj.getSpec().getJobTemplate().getSpec().getTemplate();
        assertThat(pod.getSpec().getRestartPolicy()).isEqualTo("Never");
        assertThat(pod.getSpec().getContainers()).hasSize(1);
        assertThat(pod.getMetadata().getAnnotations())
                .containsEntry(BackupWorkloadBuilder.CONFIG_HASH_ANNOTATION, "hash123");
    }

    @Test
    void backupCronJobAddsSchemaSidecarWhenRequested() {
        CronJob cj = builder.buildBackupCronJob(backupCr(),
                new ResolvedBackupEndpoint("broker:9092", null, "http://apicurio"),
                "hash123", true, ownerRef);
        assertThat(cj.getSpec().getJobTemplate().getSpec().getTemplate().getSpec().getContainers())
                .hasSize(2);
    }

    @Test
    void restoreJobNeverRetries() {
        KafkaRestore cr = new KafkaRestore();
        cr.setMetadata(new ObjectMetaBuilder().withName("r1").withNamespace("kafka").build());
        cr.setSpec(new KafkaRestoreSpec());

        Job job = builder.buildRestoreJob(cr,
                new ResolvedBackupEndpoint("broker:9092", null, null), s3(), false, "r1", ownerRef);
        assertThat(job.getSpec().getBackoffLimit()).isZero();
        assertThat(job.getSpec().getTemplate().getSpec().getInitContainers()).isEmpty();
        assertThat(job.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs())
                .contains("restore");
    }

    @Test
    void restoreJobAddsSchemaImportInitContainer() {
        KafkaRestore cr = new KafkaRestore();
        cr.setMetadata(new ObjectMetaBuilder().withName("r1").withNamespace("kafka").build());
        cr.setSpec(new KafkaRestoreSpec());

        Job job = builder.buildRestoreJob(cr,
                new ResolvedBackupEndpoint("broker:9092", null, "http://apicurio"),
                s3(), true, "r1", ownerRef);
        assertThat(job.getSpec().getTemplate().getSpec().getInitContainers()).hasSize(1);
    }

    @Test
    void validationJobRunsTheGivenValidateArgs() {
        KafkaBackupValidation cr = new KafkaBackupValidation();
        cr.setMetadata(new ObjectMetaBuilder().withName("v1").withNamespace("kafka").build());
        cr.setSpec(new KafkaBackupValidationSpec());

        Job job = builder.buildValidationJob(cr, s3(),
                List.of("validate", "--path", "s3://bk", "--backup-id", "x"), "v1", ownerRef);
        assertThat(job.getSpec().getBackoffLimit()).isEqualTo(1);
        assertThat(job.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs())
                .containsExactly("validate", "--path", "s3://bk", "--backup-id", "x");
    }
}
