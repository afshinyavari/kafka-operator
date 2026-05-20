package se.afshin.yavari.kafka.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.Comparator;
import java.util.List;

/**
 * Resolves the AdminClient bootstrap address for a referenced KafkaCluster by
 * finding the alphabetically-first broker node pool in the namespace and
 * pointing at its headless service on the INTERNAL listener (port 9092).
 *
 * <p>AdminClient only needs one reachable broker; Kafka metadata discovery does
 * the rest, so we don't need to enumerate every broker pool.
 */
@ApplicationScoped
public class BrokerBootstrapResolver {

    public static final int INTERNAL_PORT = 9092;

    @Inject
    KubernetesClient client;

    /**
     * @return a bootstrap address like {@code my-pool-headless.my-ns.svc.cluster.local:9092}
     * @throws BrokerPoolNotFoundException if no broker pool exists for this cluster
     */
    public String resolve(String clusterName, String namespace) {
        List<KafkaNodePool> pools = client.resources(KafkaNodePool.class)
                .inNamespace(namespace)
                .withLabel(KafkaNodePool.CLUSTER_LABEL, clusterName)
                .list().getItems();

        return pools.stream()
                .filter(p -> p.getSpec() != null
                        && p.getSpec().getRoles() != null
                        && p.getSpec().getRoles().contains(NodeRole.BROKER))
                .min(Comparator.comparing(p -> p.getMetadata().getName()))
                .map(p -> p.getMetadata().getName() + "-headless." + namespace
                        + ".svc.cluster.local:" + INTERNAL_PORT)
                .orElseThrow(() -> new BrokerPoolNotFoundException(
                        "No broker KafkaNodePool labeled " + KafkaNodePool.CLUSTER_LABEL
                                + "=" + clusterName + " found in namespace " + namespace));
    }

    public static class BrokerPoolNotFoundException extends RuntimeException {
        public BrokerPoolNotFoundException(String message) { super(message); }
    }
}
