package se.afshin.yavari.kafka.operator.upgrade;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator;
import se.afshin.yavari.kafka.operator.rolling.RollTracker;

import java.util.List;
import java.util.stream.Collectors;

@Path("/operator/upgrade-phase")
@ApplicationScoped
public class UpgradePhaseResource {

    @Inject KubernetesClient client;
    @Inject RollTracker rollTracker;
    @ConfigProperty(name = "kafka.cluster.id") String localClusterId;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String get() {
        boolean anyPodSetRolling = client.resources(KafkaPodSet.class).inAnyNamespace()
                .list().getItems().stream()
                .anyMatch(ps -> ps.getStatus() != null
                        && ps.getStatus().getCurrentRollingPod() != null
                        && !ps.getStatus().getCurrentRollingPod().isEmpty());

        // CrossClusterRollCoordinator polls this endpoint from a *successor* cluster that
        // wants to roll. We flip to ROLLING while THIS cluster's local proxy or Apicurio
        // Deployment is mid-roll, but only when this cluster is actually a target — a
        // non-target cluster has no local Deployment to roll, so it must not falsely gate
        // downstream clusters. Post-merge the proxy and Apicurio are sub-specs on
        // KafkaCluster, so we iterate the cluster CRs and check each component's local
        // Deployment by its fixed name.
        boolean anyDeploymentRolling = client.resources(KafkaCluster.class).inAnyNamespace()
                .list().getItems().stream()
                .filter(this::isLocalATarget)
                .anyMatch(this::anyComponentDeploymentMidRoll);

        // rollTracker.isAnyRolling() flips true for the brief window between "operator
        // decided to roll" and "Kubernetes status fields actually reflect it" — without it,
        // fast rolls (sub-second on tiny clusters) slip past the status-based check, and
        // KafkaPodSet rolls don't surface in etcd at all on the success path (status is
        // patched only after rollPod returns, with currentRollingPod cleared back to "").
        String phase = (anyPodSetRolling || anyDeploymentRolling || rollTracker.isAnyRolling())
                ? "ROLLING" : clusterUpgradePhase();
        return "{\"clusterId\":\"" + localClusterId + "\",\"upgradePhase\":\"" + phase + "\"}";
    }

    private boolean isLocalATarget(KafkaCluster cr) {
        if (cr.getSpec().getProxy() == null && cr.getSpec().getApicurio() == null) return false;
        // No targetClusters field anymore: a cluster is a target iff its id appears in
        // spec.clusters. An empty/missing clusters list (legacy fixture) defaults to true.
        List<ClusterEntry> clusters = cr.getSpec().getClusters();
        if (clusters == null || clusters.isEmpty()) return true;
        return clusters.stream().map(ClusterEntry::getId)
                .collect(Collectors.toSet()).contains(localClusterId);
    }

    private boolean anyComponentDeploymentMidRoll(KafkaCluster cr) {
        String ns = cr.getMetadata().getNamespace();
        // Proxy Deployment uses the fixed PROXY_NAME; Apicurio uses APICURIO_NAME. Both
        // names are constants on the orchestrator classes — keep this in sync if they ever
        // become per-cluster suffixed.
        if (cr.getSpec().getProxy() != null
                && isDeploymentMidRoll(ns, KafkaProxyOrchestrator.PROXY_NAME)) {
            return true;
        }
        if (cr.getSpec().getApicurio() != null
                && isDeploymentMidRoll(ns, ApicurioOrchestrator.APICURIO_NAME)) {
            return true;
        }
        return false;
    }

    private boolean isDeploymentMidRoll(String namespace, String name) {
        Deployment dep = client.apps().deployments().inNamespace(namespace).withName(name).get();
        if (dep == null || dep.getStatus() == null) return false;
        DeploymentStatus s = dep.getStatus();
        Integer desired = dep.getSpec().getReplicas();
        if (desired == null) desired = 1;
        Long observedGen = s.getObservedGeneration();
        Long currentGen = dep.getMetadata().getGeneration();
        if (observedGen != null && currentGen != null && observedGen < currentGen) return true;
        Integer updated = s.getUpdatedReplicas();
        if (updated == null || updated < desired) return true;
        Integer available = s.getAvailableReplicas();
        if (available == null || available < desired) return true;
        return false;
    }

    private String clusterUpgradePhase() {
        return client.resources(KafkaCluster.class).inAnyNamespace()
                .list().getItems().stream().findFirst()
                .map(kc -> kc.getStatus() != null && kc.getStatus().getUpgradePhase() != null
                        ? kc.getStatus().getUpgradePhase() : "IDLE")
                .orElse("IDLE");
    }
}
