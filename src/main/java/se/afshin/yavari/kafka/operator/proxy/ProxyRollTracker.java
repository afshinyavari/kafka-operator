package se.afshin.yavari.kafka.operator.proxy;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process record of "I've just initiated a proxy roll".
 *
 * <p>Why this exists: {@link se.afshin.yavari.kafka.operator.upgrade.UpgradePhaseResource}
 * also inspects the local Deployment's {@code status} fields to decide whether to report
 * {@code ROLLING}, but with {@code replicas: 1} and default rolling-update surge the
 * Deployment never visibly transitions through {@code availableReplicas < desired} — the
 * old pod stays Available the entire time the new pod is starting. That status-based
 * check can therefore miss sub-second roll windows entirely, and a successor cluster
 * polling cluster A's endpoint would see {@code IDLE} and race straight into its own
 * roll. This tracker is the explicit "I just kicked off a roll" signal that closes that
 * gap: the reconciler marks the proxy as rolling immediately before {@code serverSideApply},
 * and the entry self-expires after a conservative timeout so the system can't get stuck.
 */
@ApplicationScoped
public class ProxyRollTracker {

    /** How long after marking a roll we still report ROLLING, even without a follow-up
     *  reconcile to clear the entry. Long enough for any reasonable proxy startup
     *  (Kroxylicious typically settles in ~5–15s) plus margin; short enough that a stuck
     *  operator doesn't permanently block successor clusters. */
    private static final long DEFAULT_TTL_MS = 60_000L;

    private final ConcurrentHashMap<String, Long> rollExpiry = new ConcurrentHashMap<>();

    /** Mark the named proxy as actively rolling for the next DEFAULT_TTL_MS. Idempotent —
     *  overwrites any prior expiry, which is exactly what we want when a second consecutive
     *  roll is triggered before the previous one self-expires. */
    public void markRolling(String namespace, String name) {
        rollExpiry.put(key(namespace, name), System.currentTimeMillis() + DEFAULT_TTL_MS);
    }

    /** Clear the named proxy's roll marker. Called by the reconciler after a successful
     *  reconcile pass where the Deployment is observed stable. Safe to call when no entry
     *  exists. */
    public void markComplete(String namespace, String name) {
        rollExpiry.remove(key(namespace, name));
    }

    /** True if any tracked proxy is still within its rolling window. Lazily purges
     *  expired entries so successor clusters aren't blocked by stale records left
     *  behind by an operator restart or a reconciler crash. */
    public boolean isAnyRolling() {
        long now = System.currentTimeMillis();
        rollExpiry.entrySet().removeIf(e -> e.getValue() < now);
        return !rollExpiry.isEmpty();
    }

    private static String key(String namespace, String name) {
        return namespace + "/" + name;
    }
}
