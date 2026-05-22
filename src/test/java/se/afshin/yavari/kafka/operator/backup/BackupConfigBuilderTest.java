package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupCompression;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackup;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupSpec;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BackupConfigBuilderTest {

    private final BackupConfigBuilder builder = new BackupConfigBuilder();

    private KafkaBackup cr() {
        KafkaBackup cr = new KafkaBackup();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-backup").withNamespace("kafka").build());
        KafkaBackupSpec spec = new KafkaBackupSpec();
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket("my-bucket");
        s3.setRegion("eu-north-1");
        s3.setEndpoint("http://minio:9000");
        s3.setPathStyleAccess(true);
        s3.setCredentialsSecretRef("s3-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        spec.setStorage(storage);
        spec.setCompression(BackupCompression.ZSTD);
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void rendersBackupModeWithTlsSecurityBlock() {
        ResolvedBackupEndpoint endpoint =
                new ResolvedBackupEndpoint("broker-h.kafka.svc:9092", "broker-tls", null);
        String yaml = builder.build(cr(), endpoint);

        assertThat(yaml).contains("mode: backup");
        assertThat(yaml).contains("backup_id: \"my-backup\"");
        assertThat(yaml).contains("- \"broker-h.kafka.svc:9092\"");
        assertThat(yaml).contains("security_protocol: SSL");
        assertThat(yaml).contains("ssl_ca_location: \"/etc/kafka-backup/tls/ca.crt\"");
        assertThat(yaml).contains("ssl_key_location: \"/etc/kafka-backup/tls/tls.key\"");
        assertThat(yaml).contains("backend: s3");
        assertThat(yaml).contains("bucket: \"my-bucket\"");
        assertThat(yaml).contains("endpoint: \"http://minio:9000\"");
        assertThat(yaml).contains("path_style_access: true");
        assertThat(yaml).contains("compression: zstd");
        assertThat(yaml).contains("- \"__consumer_offsets\"");
    }

    @Test
    void omitsSecurityBlockForPlaintextCluster() {
        ResolvedBackupEndpoint endpoint =
                new ResolvedBackupEndpoint("broker-h:9092", null, null);
        assertThat(builder.build(cr(), endpoint)).doesNotContain("security:");
    }

    @Test
    void rendersAdditionalConfigIntoBackupSection() {
        KafkaBackup cr = cr();
        cr.getSpec().setAdditionalConfig(Map.of("segment_max_bytes", "1048576"));
        cr.getSpec().setCompressionLevel(5);
        String yaml = builder.build(cr, new ResolvedBackupEndpoint("b:9092", null, null));

        assertThat(yaml).contains("compression_level: 5");
        assertThat(yaml).contains("segment_max_bytes: \"1048576\"");
    }

    @Test
    void neverInlinesStorageCredentials() {
        String yaml = builder.build(cr(), new ResolvedBackupEndpoint("b:9092", null, null));
        // The credentials Secret is referenced only by mounted env vars, never rendered.
        assertThat(yaml).doesNotContain("accessKeyId");
        assertThat(yaml).doesNotContain("secretAccessKey");
    }
}
