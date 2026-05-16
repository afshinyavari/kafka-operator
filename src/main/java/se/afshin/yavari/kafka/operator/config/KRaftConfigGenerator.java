package se.afshin.yavari.kafka.operator.config;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

@ApplicationScoped
public class KRaftConfigGenerator {

    /**
     * Controller node IDs start at 1000 to avoid collision with broker IDs.
     * Cluster at index i gets controller nodeId = CONTROLLER_BASE + i.
     */
    public static final int CONTROLLER_BASE = 1000;

    /**
     * Broker node IDs: clusterIndex * BROKER_MULTIPLIER + poolLocalIndex.
     * Supports up to 100 brokers per cluster before colliding with the next cluster's range.
     */
    public static final int BROKER_MULTIPLIER = 100;

    public int controllerNodeId(int clusterIndex) {
        return CONTROLLER_BASE + clusterIndex;
    }

    public int brokerNodeId(int clusterIndex, int poolLocalIndex) {
        return clusterIndex * BROKER_MULTIPLIER + poolLocalIndex;
    }

    /**
     * Returns the 0-based index of the given clusterId within spec.clusters.
     * Throws IllegalArgumentException if the id is not found.
     */
    public int clusterIndex(KafkaClusterSpec spec, String clusterId) {
        List<ClusterEntry> clusters = spec.getClusters();
        for (int i = 0; i < clusters.size(); i++) {
            if (clusterId.equals(clusters.get(i).getId())) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                "Cluster id '" + clusterId + "' not found in spec.clusters. "
                + "Ensure KAFKA_CLUSTER_ID matches one of: "
                + clusters.stream().map(ClusterEntry::getId).toList());
    }

    /**
     * Builds the controller.quorum.voters string for all clusters.
     * Format: "1000@ctrl-a:9093,1001@ctrl-b:9093,1002@ctrl-c:9093"
     */
    public String buildQuorumVoters(KafkaClusterSpec spec) {
        List<ClusterEntry> clusters = spec.getClusters();
        if (clusters.isEmpty()) {
            throw new IllegalArgumentException("spec.clusters must not be empty");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < clusters.size(); i++) {
            ClusterEntry entry = clusters.get(i);
            String addr = entry.getControllerAdvertisedAddress();
            if (addr == null || addr.isBlank()) {
                throw new IllegalArgumentException(
                        "controllerAdvertisedAddress is required for cluster id '" + entry.getId() + "'");
            }
            if (sb.length() > 0) sb.append(',');
            sb.append(controllerNodeId(i)).append('@').append(addr);
        }
        return sb.toString();
    }

    /**
     * Derives a deterministic 22-character KRaft cluster ID from the CR name and namespace.
     * Using name+namespace (not UID) so the same ID is produced on every cluster when the
     * same KafkaCluster CR is applied via GitOps — each cluster assigns its own UID.
     */
    public String clusterIdFrom(KafkaCluster cr) {
        String seed = cr.getMetadata().getNamespace() + "/" + cr.getMetadata().getName();
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(seed.getBytes(StandardCharsets.UTF_8));
            // Take first 16 bytes → encode as base64url (22 chars without padding)
            return Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(hash).substring(0, 22);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
