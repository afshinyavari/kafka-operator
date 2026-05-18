package se.afshin.yavari.kroxy.auth;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges the JwtGroupFilter (which parses JWT groups from SASL auth bytes)
 * with JwtGroupSaslSubjectBuilder (which builds the Subject after SASL completes).
 * Keyed by the JWT sub claim, which matches the SASL authorizationId.
 */
public final class JwtGroupStore {

    private static final ConcurrentHashMap<String, Set<String>> STORE = new ConcurrentHashMap<>();

    private JwtGroupStore() {}

    public static void put(String sub, Set<String> groups) {
        STORE.put(sub, groups);
    }

    public static Set<String> get(String sub) {
        return STORE.getOrDefault(sub, Set.of());
    }

    public static Set<String> getAndRemove(String sub) {
        return STORE.remove(sub);
    }
}
