package se.afshin.yavari.kafka.ui.cluster;

/**
 * Connection details for one logical Kafka cluster (one KafkaCluster CR).
 *
 * <p>The brokers backing a single CR may be spread across multiple Kubernetes
 * clusters via Submariner MCS — that distribution is invisible to the UI.
 * Submariner Lighthouse local-prefers a reachable replica when resolving the
 * {@code .svc.clusterset.local} hostnames.
 */
public record ClusterCoordinates(
        String id,
        String namespace,
        String bootstrapUrl,
        String apicurioUrl) {
}
