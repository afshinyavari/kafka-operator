package se.afshin.yavari.kafka.operator.connect;

import jakarta.enterprise.context.ApplicationScoped;

import java.util.Map;
import java.util.Set;

/**
 * Drift detector for connector config. Connect masks sensitive values in
 * {@code GET /config} as {@code "********"}, so a naïve {@link Map#equals(Object)}
 * either always-PUTs or never-detects-changes.
 *
 * <p>Strategy:
 * <ol>
 *   <li>If the cached {@code observedConfigHash} matches the freshly-computed desired hash,
 *       skip the PUT regardless of what {@link #drifted} would say. This is the
 *       steady-state short-circuit — the spec hasn't changed, no need to nudge Connect.
 *   <li>Otherwise, structural diff:
 *     <ul>
 *       <li>Non-sensitive keys: direct equality.
 *       <li>Sensitive keys (suffix matches): if actual is {@code "********"} → assume equal;
 *           if actual is {@code ${...}} placeholder → string-compare.
 *       <li>Key in actual not in desired → drift (operator owns the config).
 *     </ul>
 * </ol>
 *
 * <p>Known v1 gap: out-of-band changes to <strong>only</strong> a sensitive value
 * cannot be detected from REST — Connect masks identically. Spec remains source of
 * truth; the operator re-PUTs on operator restart (because the cached hash lives in CR
 * status and is cleared on startup).
 */
@ApplicationScoped
public class ConnectorDriftDetector {

    /** Mask Connect substitutes for sensitive values in GET responses. */
    public static final String MASK = "********";

    /** Default sensitive-key suffixes. Connector developers and Connect's built-in
     *  config_def follow this convention. */
    private static final Set<String> SENSITIVE_SUFFIXES = Set.of(
            "password", "secret", "token", "apikey", "api.key", "credentials");

    /** Returns true when any non-sensitive key differs, or when a sensitive key's actual
     *  value is neither masked nor a placeholder matching the desired. */
    public boolean drifted(Map<String, String> desired, Map<String, String> actual) {
        for (var e : desired.entrySet()) {
            String key = e.getKey();
            String want = e.getValue();
            String have = actual.get(key);
            if (have == null) return true;
            if (isSensitive(key)) {
                if (MASK.equals(have)) continue; // can't compare — assume equal
                if (have.startsWith("${")) {
                    if (!have.equals(want)) return true;
                    continue;
                }
                if (!have.equals(want)) return true;
            } else {
                if (!have.equals(want)) return true;
            }
        }
        for (String key : actual.keySet()) {
            if (!desired.containsKey(key)) return true;
        }
        return false;
    }

    public static boolean isSensitive(String key) {
        String lower = key.toLowerCase();
        for (String suffix : SENSITIVE_SUFFIXES) {
            if (lower.endsWith(suffix)) return true;
        }
        return false;
    }
}
