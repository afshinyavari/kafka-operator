package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CrValidatorTest {

    private static final String LOCAL_ID = "cluster-a";

    @Test
    void validateCluster_emptyClusters_fails() {
        KafkaCluster cr = cluster(spec -> spec.setClusters(List.of()));
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("clusters");
    }

    @Test
    void validateCluster_blankClusterId_fails() {
        KafkaCluster cr = cluster(spec -> {
            ClusterEntry e = entry("", "ctrl:9093");
            spec.setClusters(List.of(e));
        });
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("id");
    }

    @Test
    void validateCluster_duplicateClusterIds_fails() {
        KafkaCluster cr = cluster(spec -> spec.setClusters(List.of(
                entry(LOCAL_ID, "addr1:9093"),
                entry(LOCAL_ID, "addr2:9093"))));
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("unique");
    }

    @Test
    void validateCluster_missingControllerAddress_fails() {
        KafkaCluster cr = cluster(spec -> {
            ClusterEntry e = entry(LOCAL_ID, "");
            spec.setClusters(List.of(e));
        });
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("controllerAdvertisedAddress");
        assertThat(result.message()).contains(LOCAL_ID);
    }

    @Test
    void validateCluster_localClusterIdNotInSpec_fails() {
        KafkaCluster cr = cluster(spec -> spec.setClusters(List.of(entry("other", "addr:9093"))));
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains(LOCAL_ID);
        assertThat(result.message()).contains("KAFKA_CLUSTER_ID");
    }

    @Test
    void validateCluster_unknownRollOrderId_fails() {
        KafkaCluster cr = cluster(spec -> {
            spec.setClusters(List.of(entry(LOCAL_ID, "addr:9093")));
            spec.setClusterRollOrder(List.of(LOCAL_ID, "unknown-id"));
        });
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("unknown-id");
    }

    @Test
    void validateCluster_blankKafkaImage_fails() {
        KafkaCluster cr = cluster(spec -> {
            spec.setClusters(List.of(entry(LOCAL_ID, "addr:9093")));
            spec.setKafkaImage("");
        });
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("kafkaImage");
    }

    @Test
    void validateCluster_versionDowngrade_fails() {
        KafkaCluster cr = cluster(spec -> {
            spec.setClusters(List.of(entry(LOCAL_ID, "addr:9093")));
            spec.setKafkaVersion("3.8");
        });
        KafkaClusterStatus status = new KafkaClusterStatus();
        status.setCurrentKafkaVersion("4.0");
        cr.setStatus(status);

        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).containsIgnoringCase("downgrade");
    }

    @Test
    void validateCluster_metadataVersionDowngrade_fails() {
        KafkaCluster cr = cluster(spec -> {
            spec.setClusters(List.of(entry(LOCAL_ID, "addr:9093")));
            spec.setTargetMetadataVersion(10);
        });
        KafkaClusterStatus status = new KafkaClusterStatus();
        status.setCurrentMetadataVersion(20);
        cr.setStatus(status);

        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).containsIgnoringCase("metadata.version downgrade");
    }

    @Test
    void validateCluster_valid_ok() {
        KafkaCluster cr = cluster(spec -> spec.setClusters(List.of(entry(LOCAL_ID, "addr:9093"))));
        var result = CrValidator.validateKafkaCluster(cr, LOCAL_ID);
        assertThat(result.valid()).isTrue();
    }

    @Test
    void validatePool_emptyRoles_fails() {
        KafkaNodePool pool = pool(List.of());
        var result = CrValidator.validateKafkaNodePool(pool);
        assertThat(result.valid()).isFalse();
        assertThat(result.message()).contains("roles");
    }

    @Test
    void validatePool_valid_ok() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));
        var result = CrValidator.validateKafkaNodePool(pool);
        assertThat(result.valid()).isTrue();
    }

    // --- helpers ---

    private interface SpecCustomizer { void customize(KafkaClusterSpec spec); }

    private KafkaCluster cluster(SpecCustomizer customizer) {
        KafkaCluster cr = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("my-cluster");
        cr.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setKafkaImage("kafka:4.0.0");
        customizer.customize(spec);
        cr.setSpec(spec);
        return cr;
    }

    private ClusterEntry entry(String id, String addr) {
        ClusterEntry e = new ClusterEntry();
        e.setId(id);
        e.setControllerAdvertisedAddress(addr);
        return e;
    }

    private KafkaNodePool pool(List<NodeRole> roles) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("brokers");
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(roles);
        spec.setReplicas(1);
        pool.setSpec(spec);
        return pool;
    }
}
