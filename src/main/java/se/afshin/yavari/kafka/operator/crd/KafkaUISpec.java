package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaUISpec {
    private String image = "kafka-ui:dev";
    private String imagePullPolicy = "IfNotPresent";
    private int replicas = 1;

    private KafkaUIOidcConfig oidc;
    private KafkaUITlsConfig tls = new KafkaUITlsConfig();
    private KafkaUIDiscoveryConfig discovery = new KafkaUIDiscoveryConfig();
    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();
    private KafkaUIProbesConfig probes = new KafkaUIProbesConfig();
    private KafkaUIServiceConfig service = new KafkaUIServiceConfig();
    private KafkaUIIngressConfig ingress = new KafkaUIIngressConfig();
    private List<KafkaUIEnvVar> env = List.of();

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

    public KafkaUIServiceConfig getService() { return service; }
    public void setService(KafkaUIServiceConfig service) { this.service = service; }

    public KafkaUIIngressConfig getIngress() { return ingress; }
    public void setIngress(KafkaUIIngressConfig ingress) { this.ingress = ingress; }

    public List<KafkaUIEnvVar> getEnv() { return env; }
    public void setEnv(List<KafkaUIEnvVar> env) { this.env = env; }
}
