package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Min;
import io.fabric8.generator.annotation.ValidationRule;
import io.fabric8.kubernetes.api.model.ResourceRequirements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaNodePoolSpec {

    /** Roles this pool's nodes fulfil. Any non-empty subset of [CONTROLLER, BROKER]. */
    @ValidationRule(
        value = "self.size() >= 1",
        message = "at least one role must be specified (BROKER or CONTROLLER)"
    )
    private List<NodeRole> roles = new ArrayList<>();

    @Min(1)
    private int replicas = 1;

    private StorageSpec storage = new StorageSpec();
    private ResourceRequirements resources = new ResourceRequirements();

    /** Pool-level Kafka config overrides — merged on top of KafkaCluster.spec.config. */
    private Map<String, String> config = new HashMap<>();

    /**
     * Node label key whose value is used as the Kafka broker.rack.
     * E.g. "topology.kubernetes.io/zone". If null, rack awareness is disabled for this pool.
     */
    private String rackTopologyKey;

    /**
     * Optional override for the pre-provisioned TLS secret (cert-manager / mcs-setup) that
     * holds this pool's broker cert when KafkaCluster.spec.proxyMtls.enabled=true. Must follow
     * the cert-manager convention (tls.crt + tls.key + ca.crt). Defaults to "{poolName}-broker-tls".
     */
    private String brokerCertSecretRef;

    public List<NodeRole> getRoles() { return roles; }
    public void setRoles(List<NodeRole> roles) { this.roles = roles; }

    public int getReplicas() { return replicas; }
    public void setReplicas(int replicas) { this.replicas = replicas; }

    public StorageSpec getStorage() { return storage; }
    public void setStorage(StorageSpec storage) { this.storage = storage; }

    public ResourceRequirements getResources() { return resources; }
    public void setResources(ResourceRequirements resources) { this.resources = resources; }

    public Map<String, String> getConfig() { return config; }
    public void setConfig(Map<String, String> config) { this.config = config; }

    public String getRackTopologyKey() { return rackTopologyKey; }
    public void setRackTopologyKey(String rackTopologyKey) { this.rackTopologyKey = rackTopologyKey; }

    public String getBrokerCertSecretRef() { return brokerCertSecretRef; }
    public void setBrokerCertSecretRef(String brokerCertSecretRef) { this.brokerCertSecretRef = brokerCertSecretRef; }
}
