package se.afshin.yavari.kafka.operator.infra;

import se.afshin.yavari.kafka.operator.crd.McsConfig;

import java.util.List;

/**
 * MCS placement gate shared by reconcilers whose CRs carry {@code spec.mcs} + {@code
 * spec.targetClusters} (KafkaConnect, MirrorMaker2, KafkaUI, ...). Encapsulates the
 * decision logic so each reconciler only has to map the {@link Decision} onto its own
 * status enum.
 */
public final class McsPlacement {

    private McsPlacement() {}

    public enum Decision {
        /** MCS disabled or targetClusters empty — reconcile here. */
        PROCEED,
        /** MCS enabled and local cluster not in targetClusters — skip silently. */
        SKIP,
        /** {@code spec.targetClusters} is set but {@code spec.mcs.enabled} is false. */
        INVALID_TARGETS_WITHOUT_MCS,
        /** {@code spec.mcs.enabled} is true but {@code spec.targetClusters} is empty. */
        INVALID_MCS_WITHOUT_TARGETS,
    }

    /**
     * Decide whether the current reconciler invocation should run on the local cluster.
     *
     * <p>Connect/MM2 historically only checked PROCEED vs SKIP and tolerated targetClusters
     * without mcs.enabled. Callers that don't care about the validation cases can simply
     * treat {@code INVALID_*} as PROCEED.
     */
    public static Decision decide(McsConfig mcs, List<String> targetClusters, String localClusterId) {
        boolean mcsEnabled = mcs != null && mcs.isEnabled();
        boolean targetsSet = targetClusters != null && !targetClusters.isEmpty();

        if (!mcsEnabled && targetsSet) {
            return Decision.INVALID_TARGETS_WITHOUT_MCS;
        }
        if (mcsEnabled && !targetsSet) {
            return Decision.INVALID_MCS_WITHOUT_TARGETS;
        }
        if (mcsEnabled && !targetClusters.contains(localClusterId)) {
            return Decision.SKIP;
        }
        return Decision.PROCEED;
    }
}
