package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpoint;

import java.util.Objects;

/**
 * Refuses to apply a KafkaConnect CR when another CR in the same namespace targets the
 * same Kafka cluster with the same effective {@code group.id}. Two Connect Deployments
 * sharing a groupId against one Kafka cluster would silently form a single Connect
 * worker group with split internal state — an almost-always-wrong configuration that
 * is otherwise extremely hard to debug.
 */
@ApplicationScoped
public class ConnectGroupIdGuard {

    @Inject KubernetesClient client;

    public record Conflict(String otherCrName) {}

    /** Returns a {@link Conflict} when {@code cr}'s effective groupId collides with
     *  another KafkaConnect CR targeting the same Kafka cluster; null otherwise. */
    public Conflict check(KafkaConnect cr) {
        String namespace = cr.getMetadata().getNamespace();
        String myName = cr.getMetadata().getName();
        String myGroupId = cr.resolvedGroupId();
        KafkaEndpoint myEndpoint = cr.getSpec().getKafkaClusterRef();

        var others = client.resources(KafkaConnect.class).inNamespace(namespace).list().getItems();
        for (KafkaConnect other : others) {
            if (other.getMetadata().getName().equals(myName)) continue;
            if (!Objects.equals(other.resolvedGroupId(), myGroupId)) continue;
            if (sameKafkaCluster(myEndpoint, other.getSpec().getKafkaClusterRef())) {
                return new Conflict(other.getMetadata().getName());
            }
        }
        return null;
    }

    private static boolean sameKafkaCluster(KafkaEndpoint a, KafkaEndpoint b) {
        if (a == null || b == null) return false;
        if (a.hasManaged() && b.hasManaged()) {
            return Objects.equals(a.getKafkaClusterRef().getName(), b.getKafkaClusterRef().getName());
        }
        if (a.hasExternal() && b.hasExternal()) {
            return Objects.equals(a.getExternal().getBootstrap(), b.getExternal().getBootstrap());
        }
        return false;
    }
}
