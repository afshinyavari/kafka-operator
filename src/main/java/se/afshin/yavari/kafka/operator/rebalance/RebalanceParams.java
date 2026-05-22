package se.afshin.yavari.kafka.operator.rebalance;

import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceMode;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceSpec;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Immutable Cruise Control request parameters derived from a {@link KafkaRebalanceSpec}.
 * The {@link #endpointPath()} and {@link #queryString(boolean)} are deterministic — a
 * re-issued request carrying a {@code User-Task-ID} must match the original byte-for-byte
 * or Cruise Control rejects it.
 */
public record RebalanceParams(
        KafkaRebalanceMode mode,
        List<Integer> brokers,
        List<String> goals,
        boolean skipHardGoalCheck,
        boolean rebalanceDisk,
        Integer concurrentPartitionMovementsPerBroker,
        Integer concurrentLeaderMovements,
        Long replicationThrottle,
        String excludedTopics) {

    public static RebalanceParams from(KafkaRebalanceSpec spec) {
        return new RebalanceParams(
                spec.getMode() == null ? KafkaRebalanceMode.FULL : spec.getMode(),
                spec.getBrokers() == null ? List.of() : new ArrayList<>(spec.getBrokers()),
                spec.getGoals() == null ? List.of() : new ArrayList<>(spec.getGoals()),
                spec.isSkipHardGoalCheck(),
                spec.isRebalanceDisk(),
                spec.getConcurrentPartitionMovementsPerBroker(),
                spec.getConcurrentLeaderMovements(),
                spec.getReplicationThrottle(),
                spec.getExcludedTopics());
    }

    /** Cruise Control REST path for this mode. */
    public String endpointPath() {
        return switch (mode) {
            case FULL -> "/kafkacruisecontrol/rebalance";
            case ADD_BROKERS -> "/kafkacruisecontrol/add_broker";
            case REMOVE_BROKERS -> "/kafkacruisecontrol/remove_broker";
        };
    }

    /** Deterministic query string (no leading {@code ?}). */
    public String queryString(boolean dryrun) {
        StringJoiner q = new StringJoiner("&");
        q.add("dryrun=" + dryrun);
        q.add("json=true");
        if (mode != KafkaRebalanceMode.FULL && !brokers.isEmpty()) {
            q.add("brokerid=" + brokers.stream().map(String::valueOf)
                    .collect(Collectors.joining(",")));
        }
        if (!goals.isEmpty()) {
            // Encode each goal name, keep the comma separator literal (Cruise Control
            // expects a comma-delimited list).
            q.add("goals=" + goals.stream().map(RebalanceParams::enc)
                    .collect(Collectors.joining(",")));
        }
        if (skipHardGoalCheck) {
            q.add("skip_hard_goal_check=true");
        }
        if (rebalanceDisk && mode == KafkaRebalanceMode.FULL) {
            q.add("rebalance_disk=true");
        }
        if (concurrentPartitionMovementsPerBroker != null) {
            q.add("concurrent_partition_movements_per_broker=" + concurrentPartitionMovementsPerBroker);
        }
        if (concurrentLeaderMovements != null) {
            q.add("concurrent_leader_movements=" + concurrentLeaderMovements);
        }
        if (replicationThrottle != null) {
            q.add("replication_throttle=" + replicationThrottle);
        }
        if (excludedTopics != null && !excludedTopics.isBlank()) {
            q.add("excluded_topics=" + enc(excludedTopics));
        }
        return q.toString();
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }
}
