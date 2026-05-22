package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.EnvVar;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.AzureStorageConfig;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.GcsStorageConfig;
import se.afshin.yavari.kafka.operator.crd.PvcStorageConfig;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BackupStorageMountsTest {

    private final BackupStorageMounts mounts = new BackupStorageMounts();

    private BackupStorageSpec s3() {
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket("backups");
        s3.setPrefix("prod/");
        s3.setRegion("eu-north-1");
        s3.setEndpoint("http://minio:9000");
        s3.setCredentialsSecretRef("s3-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        return storage;
    }

    private BackupStorageSpec pvc() {
        PvcStorageConfig pvc = new PvcStorageConfig();
        pvc.setClaimName("backup-pvc");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setPvc(pvc);
        return storage;
    }

    private BackupStorageSpec gcs() {
        GcsStorageConfig gcs = new GcsStorageConfig();
        gcs.setBucket("backups");
        gcs.setCredentialsSecretRef("gcs-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setGcs(gcs);
        return storage;
    }

    private BackupStorageSpec azure() {
        AzureStorageConfig az = new AzureStorageConfig();
        az.setContainer("backups");
        az.setCredentialsSecretRef("az-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setAzure(az);
        return storage;
    }

    private EnvVar byName(List<EnvVar> env, String name) {
        return env.stream().filter(e -> e.getName().equals(name)).findFirst().orElse(null);
    }

    @Test
    void s3WiringUsesSecretBackedEnvAndNoVolumes() {
        BackupStorageMounts.StorageWiring wiring = mounts.forBackupTool(s3());
        assertThat(wiring.volumes()).isEmpty();
        assertThat(wiring.mounts()).isEmpty();

        EnvVar accessKey = byName(wiring.env(), "AWS_ACCESS_KEY_ID");
        assertThat(accessKey).isNotNull();
        assertThat(accessKey.getValue()).isNull();
        assertThat(accessKey.getValueFrom().getSecretKeyRef().getName()).isEqualTo("s3-creds");
        assertThat(byName(wiring.env(), "AWS_ENDPOINT").getValue()).isEqualTo("http://minio:9000");
        assertThat(byName(wiring.env(), "AWS_ALLOW_HTTP").getValue()).isEqualTo("true");
    }

    @Test
    void pvcWiringMountsTheClaimAtTheStorageRoot() {
        BackupStorageMounts.StorageWiring wiring = mounts.forBackupTool(pvc());
        assertThat(wiring.env()).isEmpty();
        assertThat(wiring.volumes()).hasSize(1);
        assertThat(wiring.volumes().get(0).getPersistentVolumeClaim().getClaimName())
                .isEqualTo("backup-pvc");
        assertThat(wiring.mounts().get(0).getMountPath()).isEqualTo("/backup");
    }

    @Test
    void gcsWiringMountsTheKeyFileAndSetsCredentialEnv() {
        BackupStorageMounts.StorageWiring wiring = mounts.forBackupTool(gcs());
        assertThat(wiring.volumes()).hasSize(1);
        assertThat(byName(wiring.env(), "GOOGLE_APPLICATION_CREDENTIALS").getValue())
                .isEqualTo("/etc/kafka-backup/gcs/key.json");
    }

    @Test
    void appendStorageBlockRendersBackendKeys() {
        StringBuilder s3Block = new StringBuilder();
        BackupStorageMounts.appendStorageBlock(s3Block, s3());
        assertThat(s3Block.toString()).contains("backend: s3").contains("bucket: \"backups\"");

        StringBuilder pvcBlock = new StringBuilder();
        BackupStorageMounts.appendStorageBlock(pvcBlock, pvc());
        assertThat(pvcBlock.toString()).contains("backend: filesystem").contains("path: \"/backup\"");
    }

    @Test
    void schemaLocationIsPathForPvcAndBucketPathForS3() {
        assertThat(BackupStorageMounts.schemaLocation(pvc())).isEqualTo("/backup/schemas");
        assertThat(BackupStorageMounts.schemaLocation(s3())).isEqualTo("backups/prod/schemas");
    }

    @Test
    void schemaExportSupportedOnlyForPvcAndS3() {
        assertThat(BackupStorageMounts.schemaExportSupported(s3())).isTrue();
        assertThat(BackupStorageMounts.schemaExportSupported(pvc())).isTrue();
        assertThat(BackupStorageMounts.schemaExportSupported(gcs())).isFalse();
        assertThat(BackupStorageMounts.schemaExportSupported(azure())).isFalse();
    }
}
