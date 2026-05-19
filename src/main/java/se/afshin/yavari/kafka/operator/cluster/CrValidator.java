package se.afshin.yavari.kafka.operator.cluster;

import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Business-logic validation run at the start of each reconcile loop.
 * CRD schema rules (CEL / minimum constraints) catch structural errors at apply time;
 * this class catches semantic errors that require cross-field or external-state reasoning.
 */
public class CrValidator {

    public record ValidationResult(boolean valid, String message) {
        public static ValidationResult ok() { return new ValidationResult(true, null); }
        public static ValidationResult fail(String message) { return new ValidationResult(false, message); }
    }

    public static ValidationResult validateKafkaCluster(KafkaCluster cr, String localClusterId) {
        var spec = cr.getSpec();

        if (spec.getClusters() == null || spec.getClusters().isEmpty()) {
            return ValidationResult.fail("spec.clusters must not be empty");
        }

        List<String> missingIds = spec.getClusters().stream()
                .filter(c -> c.getId() == null || c.getId().isBlank())
                .map(c -> "(unnamed entry)")
                .toList();
        if (!missingIds.isEmpty()) {
            return ValidationResult.fail("all cluster entries must have a non-empty id");
        }

        Set<String> ids = spec.getClusters().stream()
                .map(c -> c.getId())
                .collect(Collectors.toSet());
        if (ids.size() != spec.getClusters().size()) {
            return ValidationResult.fail("cluster ids must be unique within spec.clusters");
        }

        List<String> missingAddrs = spec.getClusters().stream()
                .filter(c -> c.getControllerAdvertisedAddress() == null
                        || c.getControllerAdvertisedAddress().isBlank())
                .map(c -> "cluster '" + c.getId() + "'")
                .toList();
        if (!missingAddrs.isEmpty()) {
            return ValidationResult.fail(
                    "controllerAdvertisedAddress is required for: " + String.join(", ", missingAddrs));
        }

        boolean clusterIdKnown = spec.getClusters().stream()
                .anyMatch(c -> localClusterId.equals(c.getId()));
        if (!clusterIdKnown) {
            List<String> knownIds = spec.getClusters().stream().map(c -> c.getId()).toList();
            return ValidationResult.fail(
                    "KAFKA_CLUSTER_ID '" + localClusterId + "' is not in spec.clusters "
                    + knownIds + "; update the operator's KAFKA_CLUSTER_ID or add this cluster to spec.clusters");
        }

        List<String> rollOrder = spec.getClusterRollOrder();
        if (rollOrder != null && !rollOrder.isEmpty()) {
            Set<String> knownIds = spec.getClusters().stream()
                    .map(ClusterEntry::getId).collect(Collectors.toSet());
            for (String id : rollOrder) {
                if (!knownIds.contains(id)) {
                    return ValidationResult.fail(
                            "spec.clusterRollOrder contains unknown cluster id '" + id + "'");
                }
            }
        }

        if (spec.getKafkaImage() == null || spec.getKafkaImage().isBlank()) {
            return ValidationResult.fail("spec.kafkaImage must not be blank");
        }

        var status = cr.getStatus();
        if (status != null && status.getCurrentKafkaVersion() != null) {
            if (compareVersions(spec.getKafkaVersion(), status.getCurrentKafkaVersion()) < 0) {
                return ValidationResult.fail(
                        "downgrade not allowed: cluster is on " + status.getCurrentKafkaVersion()
                        + ", cannot set kafkaVersion to " + spec.getKafkaVersion());
            }
        }

        if (status != null && status.getCurrentMetadataVersion() != null
                && spec.getTargetMetadataVersion() != null
                && spec.getTargetMetadataVersion() < status.getCurrentMetadataVersion()) {
            return ValidationResult.fail(
                    "metadata.version downgrade not allowed: current is "
                    + status.getCurrentMetadataVersion()
                    + ", cannot set targetMetadataVersion to " + spec.getTargetMetadataVersion());
        }

        return ValidationResult.ok();
    }

    private static int compareVersions(String a, String b) {
        int[] partsA = parseVersion(a);
        int[] partsB = parseVersion(b);
        int cmp = Integer.compare(partsA[0], partsB[0]);
        return cmp != 0 ? cmp : Integer.compare(partsA[1], partsB[1]);
    }

    private static int[] parseVersion(String version) {
        if (version == null || version.isBlank()) return new int[]{0, 0};
        try {
            String[] parts = version.split("\\.", 2);
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return new int[]{major, minor};
        } catch (NumberFormatException e) {
            return new int[]{0, 0};
        }
    }

    public static ValidationResult validateKafkaNodePool(KafkaNodePool pool) {
        var spec = pool.getSpec();

        if (spec.getRoles() == null || spec.getRoles().isEmpty()) {
            return ValidationResult.fail("spec.roles must not be empty");
        }

        if (spec.getReplicas() < 1) {
            return ValidationResult.fail("spec.replicas must be >= 1, got " + spec.getReplicas());
        }

        boolean isBroker = spec.getRoles().contains(NodeRole.BROKER);
        boolean isController = spec.getRoles().contains(NodeRole.CONTROLLER);
        if (!isBroker && !isController) {
            return ValidationResult.fail(
                    "spec.roles contains unrecognized roles; must include BROKER and/or CONTROLLER");
        }

        return ValidationResult.ok();
    }
}
