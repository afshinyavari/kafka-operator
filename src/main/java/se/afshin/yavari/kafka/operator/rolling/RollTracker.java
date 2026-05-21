package se.afshin.yavari.kafka.operator.rolling;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process record of "I've just initiated a roll" for any operator-managed workload —
 * brokers (KafkaPodSet), the proxy Deployment, or the Apicurio Deployment.
 *
 * <p>Why this exists: {@link se.afshin.yavari.kafka.operator.upgrade.UpgradePhaseResource}
 * also inspects local resource status to decide whether to report {@code ROLLING}, but in
 * practice that signal is unreliable for two distinct reasons:
 *
 * <ul>
 *   <li>For the proxy / Apicurio Deployments with {@code replicas: 1} and default surge,
 *       the Deployment never visibly transitions through {@code availableReplicas < desired}
 *       — the old pod stays Available the entire time the new pod is starting.
 *   <li>For {@code KafkaPodSet}, {@code status.currentRollingPod} is set in memory before
 *       a multi-minute {@code rollPod()} call and cleared back to {@code ""} on success
 *       before {@code patchStatus()} runs, so etcd only ever observes ROLLING on the
 *       failure path.
 * </ul>
 *
 * <p>Either way a successor cluster polling our endpoint would see {@code IDLE} and race
 * straight into its own roll. This tracker is the explicit "I just kicked off a roll"
 * signal that closes both gaps. Entries self-expire after a conservative timeout so the
 * system can't get stuck if a reconciler crashes mid-roll.
 */
@ApplicationScoped
public class RollTracker {

    /** How long after marking a roll we still report ROLLING, even without a follow-up
     *  reconcile to clear the entry. Long enough for any reasonable broker/proxy/Apicurio
     *  startup plus margin; short enough that a stuck operator doesn't permanently block
     *  successor clusters. */
    private static final long DEFAULT_TTL_MS = 60_000L;

    private final ConcurrentHashMap<String, Long> rollExpiry = new ConcurrentHashMap<>();

    /** Mark the named workload as actively rolling for the next DEFAULT_TTL_MS. Idempotent —
     *  overwrites any prior expiry, which is exactly what we want when a second consecutive
     *  roll is triggered before the previous one self-expires.
     *
     *  @param kind component kind ("proxy", "podset", "apicurio") — included in the key so
     *              two workloads with the same namespace/name but different kinds don't
     *              collide. */
    public void markRolling(String kind, String namespace, String name) {
        rollExpiry.put(key(kind, namespace, name), System.currentTimeMillis() + DEFAULT_TTL_MS);
    }

    /** Clear the named workload's roll marker. Called by the reconciler after a successful
     *  reconcile pass where the underlying resource is observed stable. Safe to call when
     *  no entry exists. */
    public void markComplete(String kind, String namespace, String name) {
        rollExpiry.remove(key(kind, namespace, name));
    }

    /** True if any tracked workload (of any kind) is still within its rolling window.
     *  Lazily purges expired entries so successor clusters aren't blocked by stale records
     *  left behind by an operator restart or a reconciler crash. */
    public boolean isAnyRolling() {
        long now = System.currentTimeMillis();
        rollExpiry.entrySet().removeIf(e -> e.getValue() < now);
        return !rollExpiry.isEmpty();
    }

    private static String key(String kind, String namespace, String name) {
        return kind + "/" + namespace + "/" + name;
    }
}
