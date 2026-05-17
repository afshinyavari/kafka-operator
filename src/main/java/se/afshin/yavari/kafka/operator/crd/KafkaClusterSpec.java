package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClusterSpec {

    private String kafkaImage = "kafka-ubi:4.0.0";
    private String kafkaVersion = "4.0";
    private Integer targetMetadataVersion;

    /** Ordered list of all clusters participating in the KRaft quorum.
        Index position determines the controller node ID (1000 + index). */
    @ValidationRule(
        value = "self.size() >= 1",
        message = "spec.clusters must contain at least one entry"
    )
    private List<ClusterEntry> clusters = new ArrayList<>();

    /** Shared Kafka config properties merged into all node pool configs. */
    private Map<String, String> config = new HashMap<>();

    /** When set, enables JMX metrics via the bundled jmx_prometheus_javaagent.
     *  configMapRef must name a ConfigMap in the same namespace with a jmx-config.yaml key. */
    private MetricsConfig metricsConfig;

    /** Optional ordered list of cluster IDs for sequenced rolling updates.
     *  The first cluster in the list rolls first; each subsequent cluster waits until
     *  all preceding clusters report upgradePhase=IDLE. If absent, no cross-cluster
     *  coordination is performed (existing behaviour). */
    private List<String> clusterRollOrder;

    public String getKafkaImage() { return kafkaImage; }
    public void setKafkaImage(String kafkaImage) { this.kafkaImage = kafkaImage; }

    public String getKafkaVersion() { return kafkaVersion; }
    public void setKafkaVersion(String kafkaVersion) { this.kafkaVersion = kafkaVersion; }

    public List<ClusterEntry> getClusters() { return clusters; }
    public void setClusters(List<ClusterEntry> clusters) { this.clusters = clusters; }

    public Integer getTargetMetadataVersion() { return targetMetadataVersion; }
    public void setTargetMetadataVersion(Integer targetMetadataVersion) { this.targetMetadataVersion = targetMetadataVersion; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }

    public List<String> getClusterRollOrder() { return clusterRollOrder; }
    public void setClusterRollOrder(List<String> clusterRollOrder) { this.clusterRollOrder = clusterRollOrder; }

    public MetricsConfig getMetricsConfig() { return metricsConfig; }
    public void setMetricsConfig(MetricsConfig metricsConfig) { this.metricsConfig = metricsConfig; }
}
