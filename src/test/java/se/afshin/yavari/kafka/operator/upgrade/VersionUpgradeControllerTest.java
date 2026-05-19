package se.afshin.yavari.kafka.operator.upgrade;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class VersionUpgradeControllerTest {

    private VersionUpgradeController controller;

    @BeforeEach
    void setup() {
        controller = new VersionUpgradeController();
    }

    @Test
    void emptyPods_setsIdle() {
        KafkaClusterStatus status = new KafkaClusterStatus();
        controller.reconcile(cluster("4.0", null), List.of(), "kafka", status);
        assertThat(status.getUpgradePhase()).isEqualTo("IDLE");
    }

    @Test
    void podsWithNoVersionAnnotations_setsIdle() {
        Pod p = pod(null, true, 0, "brokers");  // no KAFKA_VERSION_ANNOTATION
        KafkaClusterStatus status = new KafkaClusterStatus();
        controller.reconcile(cluster("4.0", null), List.of(p), "kafka", status);
        assertThat(status.getUpgradePhase()).isEqualTo("IDLE");
    }

    @Test
    void mixedVersions_setsRolling() {
        Pod p1 = pod("3.7", true, 0, "brokers");
        Pod p2 = pod("3.9", true, 1, "brokers");
        KafkaClusterStatus status = new KafkaClusterStatus();

        controller.reconcile(cluster("3.9", null), List.of(p1, p2), "kafka", status);

        assertThat(status.getUpgradePhase()).isEqualTo("ROLLING");
        assertThat(status.getCurrentKafkaVersion()).isEqualTo("3.7"); // min version
    }

    @Test
    void allOnTarget_noTargetMetadataVersion_setsIdle() {
        Pod p = pod("4.0", true, 0, "brokers");
        KafkaClusterStatus status = new KafkaClusterStatus();

        controller.reconcile(cluster("4.0", null), List.of(p), "kafka", status);

        assertThat(status.getUpgradePhase()).isEqualTo("IDLE");
        assertThat(status.getCurrentKafkaVersion()).isEqualTo("4.0");
    }

    @Test
    void allOnTarget_noReadyBroker_setsMetadataPending() {
        // Controller pod (nodeId >= CONTROLLER_BASE) — not a broker
        Pod ctrlPod = pod("4.0", true, KRaftConfigGenerator.CONTROLLER_BASE, "controllers");
        KafkaClusterStatus status = new KafkaClusterStatus();

        controller.reconcile(cluster("4.0", 20), List.of(ctrlPod), "kafka", status);

        assertThat(status.getUpgradePhase()).isEqualTo("METADATA_PENDING");
    }

    @Test
    void allOnTarget_notReadyBroker_setsMetadataPending() {
        // Broker pod but not Ready
        Pod brokerPod = pod("4.0", false, 0, "brokers");
        KafkaClusterStatus status = new KafkaClusterStatus();

        controller.reconcile(cluster("4.0", 20), List.of(brokerPod), "kafka", status);

        assertThat(status.getUpgradePhase()).isEqualTo("METADATA_PENDING");
    }

    // --- helpers ---

    private KafkaCluster cluster(String kafkaVersion, Integer targetMetadataVersion) {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("test-cluster");
        meta.setNamespace("kafka");
        c.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setKafkaVersion(kafkaVersion);
        spec.setTargetMetadataVersion(targetMetadataVersion);
        c.setSpec(spec);
        return c;
    }

    private Pod pod(String kafkaVersion, boolean ready, int nodeId, String poolName) {
        Pod p = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(poolName + "-" + nodeId);
        Map<String, String> labels = new java.util.HashMap<>();
        labels.put(KafkaPodSet.NODE_ID_LABEL, String.valueOf(nodeId));
        labels.put(KafkaPodSet.NODE_POOL_LABEL, poolName);
        meta.setLabels(labels);
        Map<String, String> annotations = new java.util.HashMap<>();
        if (kafkaVersion != null) {
            annotations.put(KafkaPodSet.KAFKA_VERSION_ANNOTATION, kafkaVersion);
        }
        meta.setAnnotations(annotations);
        p.setMetadata(meta);

        PodStatus status = new PodStatus();
        PodCondition readyCond = new PodConditionBuilder()
                .withType("Ready")
                .withStatus(ready ? "True" : "False")
                .build();
        status.setConditions(List.of(readyCond));
        p.setStatus(status);
        return p;
    }
}
