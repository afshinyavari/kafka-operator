package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;
import io.fabric8.generator.annotation.ValidationRule;

import java.util.List;

/** Spec for a MirrorMaker2 CR. Both source and target are independently either a
 *  managed KafkaCluster reference or an external endpoint description. At least one
 *  end must be managed by this operator — otherwise there's nowhere for the operator
 *  to run MM2 workloads. */
@ValidationRule(
        value = "has(self.source.kafkaClusterRef) || has(self.target.kafkaClusterRef)",
        message = "at least one of spec.source or spec.target must reference a managed KafkaCluster"
)
public class MirrorMaker2Spec {

    /** MM2 worker image. Default builds bundle the Apicurio schema-sync SMT JAR. */
    private String image = "mm2:dev";

    private String imagePullPolicy = "IfNotPresent";

    /** Worker replicas. When null, the reconciler picks a default: 3 when the target
     *  is a multi-cluster managed KafkaCluster (one per MCS cluster), otherwise 1. */
    private Integer replicas;

    @Required
    private Mm2Endpoint source;

    @Required
    private Mm2Endpoint target;

    private Mm2FlowConfig flow = new Mm2FlowConfig();

    /** Optional schema-mirroring SMT config. When null or enabled=false, MM2 mirrors
     *  topic data only — schemas must be replicated by other means. */
    private Mm2SchemaSyncConfig schemaSync;

    /** MCS placement gate. When mcs.enabled=true and targetClusters is set, the
     *  reconciler skips clusters not listed (status=SKIPPED). Used for N-way deployment
     *  control in multi-K8s topologies. */
    private McsConfig mcs;

    /** List of K8s cluster IDs where MM2 workers should run. When empty, derived from
     *  the target's KafkaCluster.spec.clusters (if managed) or [localClusterId]. */
    private List<String> targetClusters = List.of();

    /** Ordered list of cluster IDs for sequenced rolling updates of the MM2 Deployment.
     *  Same semantics as KafkaCluster.spec.clusterRollOrder. */
    private List<String> clusterRollOrder;

    private KafkaUIResourceRequirements resources = new KafkaUIResourceRequirements();
    private KafkaUIProbesConfig probes = new KafkaUIProbesConfig();

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }

    public String getImagePullPolicy() { return imagePullPolicy; }
    public void setImagePullPolicy(String imagePullPolicy) { this.imagePullPolicy = imagePullPolicy; }

    public Integer getReplicas() { return replicas; }
    public void setReplicas(Integer replicas) { this.replicas = replicas; }

    public Mm2Endpoint getSource() { return source; }
    public void setSource(Mm2Endpoint source) { this.source = source; }

    public Mm2Endpoint getTarget() { return target; }
    public void setTarget(Mm2Endpoint target) { this.target = target; }

    public Mm2FlowConfig getFlow() { return flow; }
    public void setFlow(Mm2FlowConfig flow) { this.flow = flow; }

    public Mm2SchemaSyncConfig getSchemaSync() { return schemaSync; }
    public void setSchemaSync(Mm2SchemaSyncConfig schemaSync) { this.schemaSync = schemaSync; }

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
}
