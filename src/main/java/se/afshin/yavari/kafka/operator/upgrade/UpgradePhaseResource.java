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
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.proxy.ProxyRollTracker;

import java.util.List;
import java.util.stream.Collectors;

@Path("/operator/upgrade-phase")
@ApplicationScoped
public class UpgradePhaseResource {

    @Inject KubernetesClient client;
    @Inject ProxyRollTracker proxyRollTracker;
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
        // wants to roll its own proxy. We flip to ROLLING while THIS cluster's local proxy
        // Deployment is mid-roll, but only when this cluster is actually a target — a
        // non-target cluster has no local Deployment to roll, so it must not falsely gate
        // downstream clusters. Post-merge the proxy is a sub-spec on KafkaCluster, so we
        // iterate the cluster CRs and check each one's local Deployment.
        boolean anyProxyRolling = client.resources(KafkaCluster.class).inAnyNamespace()
                .list().getItems().stream()
                .filter(this::isLocalATarget)
                .anyMatch(this::isLocalDeploymentMidRoll);

        // proxyRollTracker.isAnyRolling() flips true for the brief window between
        // "operator decided to roll" and "Kubernetes status fields actually reflect it" —
        // without it, fast rolls (sub-second on tiny clusters) slip past the status-based
        // check entirely and successor clusters race their own rolls in parallel.
        String phase = (anyPodSetRolling || anyProxyRolling || proxyRollTracker.isAnyRolling())
                ? "ROLLING" : clusterUpgradePhase();
        return "{\"clusterId\":\"" + localClusterId + "\",\"upgradePhase\":\"" + phase + "\"}";
    }

    private boolean isLocalATarget(KafkaCluster cr) {
        if (cr.getSpec().getProxy() == null) return false;
        // No targetClusters field anymore: a cluster is a proxy target iff its id appears
        // in spec.clusters. An empty/missing clusters list (legacy fixture) defaults to true.
        List<ClusterEntry> clusters = cr.getSpec().getClusters();
        if (clusters == null || clusters.isEmpty()) return true;
        return clusters.stream().map(ClusterEntry::getId)
                .collect(Collectors.toSet()).contains(localClusterId);
    }

    private boolean isLocalDeploymentMidRoll(KafkaCluster cr) {
        String name = cr.getMetadata().getName();
        String ns = cr.getMetadata().getNamespace();
        Deployment dep = client.apps().deployments().inNamespace(ns).withName(name).get();
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
