package se.afshin.yavari.kafka.operator.reconciler;

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
class CrValidator {

    record ValidationResult(boolean valid, String message) {
        static ValidationResult ok() { return new ValidationResult(true, null); }
        static ValidationResult fail(String message) { return new ValidationResult(false, message); }
    }

    static ValidationResult validateKafkaCluster(KafkaCluster cr, String localClusterId) {
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

        if (spec.getKafkaImage() == null || spec.getKafkaImage().isBlank()) {
            return ValidationResult.fail("spec.kafkaImage must not be blank");
        }

        return ValidationResult.ok();
    }

    static ValidationResult validateKafkaNodePool(KafkaNodePool pool) {
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
