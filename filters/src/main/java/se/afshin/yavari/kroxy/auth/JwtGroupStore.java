package se.afshin.yavari.kroxy.auth;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges the JwtGroupFilter (which parses JWT groups from SASL auth bytes)
 * with JwtGroupSaslSubjectBuilder (which builds the Subject after SASL completes).
 * Keyed by the JWT sub claim, which matches the SASL authorizationId.
 *
 * <p>Also caches the JWT's {@code preferred_username} claim for the same sub —
 * used by the audit filter to print a human-readable principal
 * ({@code user:alice}) instead of the SASL authorizationId UUID.
 */
public final class JwtGroupStore {

    private static final ConcurrentHashMap<String, Set<String>> GROUPS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> USERNAMES = new ConcurrentHashMap<>();

    private JwtGroupStore() {}

    public static void put(String sub, Set<String> groups) {
        GROUPS.put(sub, groups);
    }

    public static void putUsername(String sub, String username) {
        if (username != null && !username.isBlank()) USERNAMES.put(sub, username);
    }

    public static Set<String> get(String sub) {
        return GROUPS.getOrDefault(sub, Set.of());
    }

    /** Returns the JWT {@code preferred_username} for the given sub, or null if unknown. */
    public static String getUsername(String sub) {
        return USERNAMES.get(sub);
    }

    public static Set<String> getAndRemove(String sub) {
        return GROUPS.remove(sub);
    }
}
