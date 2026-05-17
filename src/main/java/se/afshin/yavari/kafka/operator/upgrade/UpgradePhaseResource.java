package se.afshin.yavari.kafka.operator.upgrade;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

@Path("/operator/upgrade-phase")
@ApplicationScoped
public class UpgradePhaseResource {

    @Inject KubernetesClient client;
    @ConfigProperty(name = "kafka.cluster.id") String localClusterId;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String get() {
        boolean anyRolling = client.resources(KafkaPodSet.class).inAnyNamespace()
                .list().getItems().stream()
                .anyMatch(ps -> ps.getStatus() != null
                        && ps.getStatus().getCurrentRollingPod() != null
                        && !ps.getStatus().getCurrentRollingPod().isEmpty());

        String phase = anyRolling ? "ROLLING" : clusterUpgradePhase();
        return "{\"clusterId\":\"" + localClusterId + "\",\"upgradePhase\":\"" + phase + "\"}";
    }

    private String clusterUpgradePhase() {
        return client.resources(KafkaCluster.class).inAnyNamespace()
                .list().getItems().stream().findFirst()
                .map(kc -> kc.getStatus() != null && kc.getStatus().getUpgradePhase() != null
                        ? kc.getStatus().getUpgradePhase() : "IDLE")
                .orElse("IDLE");
    }
}
