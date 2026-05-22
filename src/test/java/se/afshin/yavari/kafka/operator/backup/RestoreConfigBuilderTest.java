package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRestore;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreSpec;
import se.afshin.yavari.kafka.operator.crd.RestoreTimeWindow;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreConfigBuilderTest {

    private final RestoreConfigBuilder builder = new RestoreConfigBuilder();

    private BackupStorageSpec storage() {
        S3StorageConfig s3 = new S3StorageConfig();
        s3.setBucket("b");
        s3.setCredentialsSecretRef("s3-creds");
        BackupStorageSpec storage = new BackupStorageSpec();
        storage.setS3(s3);
        return storage;
    }

    private KafkaRestore cr() {
        KafkaRestore cr = new KafkaRestore();
        cr.setMetadata(new ObjectMetaBuilder().withName("restore-1").withNamespace("kafka").build());
        cr.setSpec(new KafkaRestoreSpec());
        return cr;
    }

    @Test
    void rendersRestoreModeWithTargetCluster() {
        String yaml = builder.build(cr(),
                new ResolvedBackupEndpoint("broker:9092", null, null), storage(), "bk-7");
        assertThat(yaml).contains("mode: restore");
        assertThat(yaml).contains("backup_id: \"bk-7\"");
        assertThat(yaml).contains("target:");
        assertThat(yaml).contains("- \"broker:9092\"");
        assertThat(yaml).contains("create_topics: true");
        assertThat(yaml).contains("dry_run: false");
        assertThat(yaml).contains("reset_consumer_offsets: false");
    }

    @Test
    void rendersPitrWindowAndTopicMapping() {
        KafkaRestore cr = cr();
        RestoreTimeWindow window = new RestoreTimeWindow();
        window.setStartMillis(1000L);
        window.setEndMillis(2000L);
        cr.getSpec().setTimeWindow(window);
        cr.getSpec().setTopicMapping(Map.of("orders", "orders-restored"));

        String yaml = builder.build(cr,
                new ResolvedBackupEndpoint("broker:9092", null, null), storage(), "bk-7");
        assertThat(yaml).contains("time_window_start: 1000");
        assertThat(yaml).contains("time_window_end: 2000");
        assertThat(yaml).contains("topic_mapping:");
        assertThat(yaml).contains("orders: \"orders-restored\"");
    }

    @Test
    void rendersConsumerGroupsWhenOffsetRestoreEnabled() {
        KafkaRestore cr = cr();
        cr.getSpec().setRestoreOffsets(true);
        cr.getSpec().setConsumerGroups(List.of("billing-consumers"));

        String yaml = builder.build(cr,
                new ResolvedBackupEndpoint("broker:9092", null, null), storage(), "bk-7");
        assertThat(yaml).contains("reset_consumer_offsets: true");
        assertThat(yaml).contains("consumer_groups:");
        assertThat(yaml).contains("- \"billing-consumers\"");
    }
}
