package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackup;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupSpec;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the reconciler's pure helpers without touching the kube client. */
class KafkaBackupReconcilerTest {

    private KafkaBackup cr(Boolean includeSchemas, String storageSecret, String oauthSecret) {
        KafkaBackup cr = new KafkaBackup();
        cr.setMetadata(new ObjectMetaBuilder()
                .withName("my-backup").withNamespace("kafka").withUid("uid-1").build());
        KafkaBackupSpec spec = new KafkaBackupSpec();
        spec.setIncludeSchemas(includeSchemas);
        spec.setSchemaRegistryAuthSecretRef(oauthSecret);
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket("b");
        s3.setCredentialsSecretRef(storageSecret);
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        spec.setStorage(storage);
        cr.setSpec(spec);
        return cr;
    }

    private ResolvedBackupEndpoint endpoint(boolean apicurio, String tlsSecret) {
        return new ResolvedBackupEndpoint("broker:9092", tlsSecret,
                apicurio ? "http://apicurio" : null);
    }

    @Test
    void includeSchemasHonoursExplicitSpecValue() {
        assertThat(KafkaBackupReconciler.resolveIncludeSchemas(
                cr(true, "s3", null), endpoint(false, null))).isTrue();
        assertThat(KafkaBackupReconciler.resolveIncludeSchemas(
                cr(false, "s3", null), endpoint(true, null))).isFalse();
    }

    @Test
    void includeSchemasDefaultsToApicurioPresence() {
        assertThat(KafkaBackupReconciler.resolveIncludeSchemas(
                cr(null, "s3", null), endpoint(true, null))).isTrue();
        assertThat(KafkaBackupReconciler.resolveIncludeSchemas(
                cr(null, "s3", null), endpoint(false, null))).isFalse();
    }

    @Test
    void secretRefsCollectsTlsStorageAndOauthSecrets() {
        assertThat(KafkaBackupReconciler.secretRefs(
                cr(true, "s3-creds", "oauth-creds"), endpoint(true, "broker-tls")))
                .containsExactlyInAnyOrder("broker-tls", "s3-creds", "oauth-creds");
    }

    @Test
    void secretRefsOmitsAbsentSecrets() {
        assertThat(KafkaBackupReconciler.secretRefs(
                cr(false, "s3-creds", null), endpoint(false, null)))
                .containsExactly("s3-creds");
    }

    @Test
    void ownerRefIsAControllerReference() {
        var ref = KafkaBackupReconciler.ownerRef(cr(null, "s3", null));
        assertThat(ref.getName()).isEqualTo("my-backup");
        assertThat(ref.getUid()).isEqualTo("uid-1");
        assertThat(ref.getController()).isTrue();
        assertThat(ref.getBlockOwnerDeletion()).isTrue();
    }
}
