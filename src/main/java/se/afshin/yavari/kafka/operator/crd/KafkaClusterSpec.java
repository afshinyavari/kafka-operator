package se.afshin.yavari.kafka.operator.crd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaClusterSpec {

    private String kafkaImage = "apache/kafka:3.7.0";
    private String kafkaVersion = "3.7";

    /** Ordered list of all clusters participating in the KRaft quorum.
        Index position determines the controller node ID (1000 + index). */
    private List<ClusterEntry> clusters = new ArrayList<>();

    /** Shared Kafka config properties merged into all node pool configs. */
    private Map<String, String> config = new HashMap<>();

    public String getKafkaImage() { return kafkaImage; }
    public void setKafkaImage(String kafkaImage) { this.kafkaImage = kafkaImage; }

    public String getKafkaVersion() { return kafkaVersion; }
    public void setKafkaVersion(String kafkaVersion) { this.kafkaVersion = kafkaVersion; }

    public List<ClusterEntry> getClusters() { return clusters; }
    public void setClusters(List<ClusterEntry> clusters) { this.clusters = clusters; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }
}
