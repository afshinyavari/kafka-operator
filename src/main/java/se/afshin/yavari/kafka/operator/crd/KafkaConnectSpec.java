package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.ArrayList;
import java.util.List;

/** Spec for a KafkaConnect CR. Provisions a distributed-mode Connect worker cluster
 *  attached to a single Kafka cluster (managed or external). Connectors are managed
 *  via sibling {@code KafkaConnector} CRs that reference this CR by name. */
public class KafkaConnectSpec {

    /** Connect worker image. Defaults to the operator-shipped {@code connect:dev}
     *  image; users supply a derived image to bake plugins into
     *  {@code /opt/kafka/connect-plugins/baked}. */
    private String image = "connect:dev";

    private String imagePullPolicy = "IfNotPresent";

    /** Worker replicas. When null, the reconciler picks a default: 3 when the attached
     *  Kafka cluster is a multi-cluster managed KafkaCluster, otherwise 1. */
    private Integer replicas;

    /** Single Kafka attachment — provides bootstrap, TLS material, and (when managed)
     *  the cluster on which internal config/offsets/status topics are auto-created. */
    @Required
    private KafkaEndpoint kafkaClusterRef;

    /** Connect {@code group.id}. Defaults to {@code connect-<metadata.name>}. Two
     *  KafkaConnect CRs sharing a groupId against the same Kafka cluster would silently
     *  form a single Connect group — the reconciler rejects this with phase=FAILED. */
    @ValidationRule(value = "self.matches('^[a-zA-Z0-9._-]{1,249}$')",
            message = "groupId must match ^[a-zA-Z0-9._-]{1,249}$")
    private String groupId;

    /** Container port + Service port for the Connect REST API. */
    @ValidationRule(value = "self >= 1024 && self <= 65535",
            message = "restPort must be in [1024, 65535]")
    private int restPort = 8083;

    /** Plugin delivery — PVC + ConfigMaps + Secrets, composable. A custom {@code image}
     *  with plugins baked into {@code /opt/kafka/connect-plugins/baked} is a fourth
     *  implicit channel. */
    private KafkaConnectPluginSources pluginSources = new KafkaConnectPluginSources();

    private KafkaConnectWorkerConfig worker = new KafkaConnectWorkerConfig();

    /** MCS placement gate. When mcs.enabled=true and targetClusters is set, the
     *  reconciler skips clusters not listed (status=SKIPPED). */
    private McsConfig mcs;

    /** List of K8s cluster IDs where Connect workers should run. When empty and the
     *  attachment is managed, derived from the KafkaCluster.spec.clusters. */
    private List<String> targetClusters = new ArrayList<>();

    /** Ordered list of cluster IDs for sequenced rolling updates of the Connect Deployment. */
    private List<String> clusterRollOrder;

    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();
    private KafkaUIProbesConfig probes = connectProbes();

    /** When set, attaches the bundled JMX exporter and creates a {@code <name>-metrics}
     *  Service + ServiceMonitor. The {@link MetricsConfig#getConfigMapRef()} field is not
     *  consulted — the operator bundles a fixed JMX exporter config. */
    private MetricsConfig metricsConfig;

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public Integer getReplicas() { return replicas; }
    public void setReplicas(Integer replicas) { this.replicas = replicas; }

    public KafkaEndpoint getKafkaClusterRef() { return kafkaClusterRef; }
    public void setKafkaClusterRef(KafkaEndpoint kafkaClusterRef) { this.kafkaClusterRef = kafkaClusterRef; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public int getRestPort() { return restPort; }
    public void setRestPort(int restPort) { this.restPort = restPort; }

    public KafkaConnectPluginSources getPluginSources() { return pluginSources; }
    public void setPluginSources(KafkaConnectPluginSources pluginSources) { this.pluginSources = pluginSources; }

    public KafkaConnectWorkerConfig getWorker() { return worker; }
    public void setWorker(KafkaConnectWorkerConfig worker) { this.worker = worker; }

    public McsConfig getMcs() { return mcs; }
    public void setMcs(McsConfig mcs) { this.mcs = mcs; }

    public List<String> getTargetClusters() { return targetClusters; }
    public void setTargetClusters(List<String> targetClusters) { this.targetClusters = targetClusters; }

    public List<String> getClusterRollOrder() { return clusterRollOrder; }
    public void setClusterRollOrder(List<String> clusterRollOrder) { this.clusterRollOrder = clusterRollOrder; }

    public KafkaUIResourceRequirements getResources() { return resources; }
    public void setResources(KafkaUIResourceRequirements resources) { this.resources = resources; }

    public KafkaUIProbesConfig getProbes() { return probes; }
    public void setProbes(KafkaUIProbesConfig probes) { this.probes = probes; }

    public MetricsConfig getMetricsConfig() { return metricsConfig; }
    public void setMetricsConfig(MetricsConfig metricsConfig) { this.metricsConfig = metricsConfig; }

    private static KafkaUIProbesConfig connectProbes() {
        KafkaUIProbesConfig p = new KafkaUIProbesConfig();
        p.setReadiness(new KafkaUIProbeConfig("/", 30, 10));
        p.setLiveness(new KafkaUIProbeConfig("/", 60, 30));
        return p;
    }
}
