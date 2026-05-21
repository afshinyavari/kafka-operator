package se.afshin.yavari.kafka.operator.crd;

import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;

import java.util.List;

public class KafkaUISpec {
    /** Fixed container port the kafka-ui image listens on. */
    public static final int PORT = 8080;

    private String image = "kafka-ui:dev";
    private String imagePullPolicy = "IfNotPresent";
    private int replicas = 1;

    private KafkaUIOidcConfig oidc;
    private KafkaUITlsConfig tls = new KafkaUITlsConfig();
    private KafkaUIDiscoveryConfig discovery = new KafkaUIDiscoveryConfig();
    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();
    private KafkaUIProbesConfig probes = new KafkaUIProbesConfig();
    private HttpExternalAccessConfig externalAccess = new HttpExternalAccessConfig();
    private List<KafkaUIEnvVar> env = List.of();
    private McsConfig mcs;
    private List<String> targetClusters = List.of();

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public KafkaUIOidcConfig getOidc() { return oidc; }
    public void setOidc(KafkaUIOidcConfig oidc) { this.oidc = oidc; }

    public KafkaUITlsConfig getTls() { return tls; }
    public void setTls(KafkaUITlsConfig tls) { this.tls = tls; }

    public KafkaUIDiscoveryConfig getDiscovery() { return discovery; }
    public void setDiscovery(KafkaUIDiscoveryConfig discovery) { this.discovery = discovery; }

    public KafkaUIResourceRequirements getResources() { return resources; }
    public void setResources(KafkaUIResourceRequirements resources) { this.resources = resources; }

    public KafkaUIProbesConfig getProbes() { return probes; }
    public void setProbes(KafkaUIProbesConfig probes) { this.probes = probes; }

    public HttpExternalAccessConfig getExternalAccess() { return externalAccess; }
    public void setExternalAccess(HttpExternalAccessConfig externalAccess) { this.externalAccess = externalAccess; }

    public List<KafkaUIEnvVar> getEnv() { return env; }
    public void setEnv(List<KafkaUIEnvVar> env) { this.env = env; }

    public McsConfig getMcs() { return mcs; }
    public void setMcs(McsConfig mcs) { this.mcs = mcs; }

    public List<String> getTargetClusters() { return targetClusters; }
    public void setTargetClusters(List<String> targetClusters) { this.targetClusters = targetClusters; }
}
