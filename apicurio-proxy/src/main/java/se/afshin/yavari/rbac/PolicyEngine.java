package se.afshin.yavari.rbac;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.x500.X500Principal;

@ApplicationScoped
public class PolicyEngine {

    public enum Action { READ, WRITE, DELETE }

    record Resource(String artifact, List<Action> actions) {}
    record Rule(List<String> roles, List<Resource> resources) {}
    /** One loaded policy file: role rules plus certificate principal → roles. Swapped atomically. */
    record Policy(List<Rule> rules, Map<String, Set<String>> principalRoles) {}

    @ConfigProperty(name = "proxy.policy.file")
    String policyFilePath;

    /** Kafka-ACL source for certificate identities (package-private for tests). */
    @Inject
    KafkaAclPolicySource acls;

    @ConfigProperty(name = "proxy.mtls.principal", defaultValue = "DN")
    MtlsPrincipal.Mode principalMode;

    /** Always read the mode through this accessor from other beans: {@code PolicyEngine} is
     *  injected as a CDI client proxy, and a field read on the proxy yields the proxy's own
     *  (null) field, not the bean's configured value. */
    public MtlsPrincipal.Mode principalMode() {
        return principalMode == null ? MtlsPrincipal.Mode.DN : principalMode;
    }

    private final AtomicReference<Policy> policy = new AtomicReference<>(new Policy(List.of(), Map.of()));
    private volatile boolean initialized = false;
    private volatile boolean running = true;
    private Thread watchThread;

    void onStart(@Observes StartupEvent event) throws Exception {
        Path path = Path.of(policyFilePath);
        reload(path);
        watchThread = new Thread(() -> watchLoop(path), "policy-watcher");
        watchThread.setDaemon(true);
        watchThread.start();
    }

    @PreDestroy
    void onStop() {
        running = false;
        if (watchThread != null) watchThread.interrupt();
    }

    boolean isInitialized() { return initialized; }

    /**
     * An OIDC identity is judged only by the role rules, with the roles from its token. A
     * certificate identity is allowed by its Kafka ACLs, or by the role rules for the roles
     * the policy file's {@code principals} section maps its principal to. A Kafka DENY on the
     * action vetoes such a role grant. With the ACL source enabled but no snapshot yet, a
     * certificate identity is denied outright, since a DENY cannot be ruled out.
     */
    public boolean isAllowed(SecurityIdentity identity, String artifact, Action action) {
        if (!MtlsPrincipal.isCertificateIdentity(identity)) {
            return isAllowed(identity.getRoles(), artifact, action);
        }
        String principal = MtlsPrincipal.principalOf(identity, principalMode());
        boolean aclsLoaded = acls != null && acls.isLoaded();
        if (!aclsLoaded && acls != null && acls.isEnabled()) return false;
        if (aclsLoaded && acls.isAllowed(principal, artifact, action)) return true;
        Set<String> mapped = rolesForPrincipal(principal);
        if (mapped.isEmpty()) return false;
        if (aclsLoaded && acls.isDenied(principal, artifact, action)) return false;
        return isAllowed(mapped, artifact, action);
    }

    /** Roles the policy file maps a certificate principal to; empty when unmapped. */
    Set<String> rolesForPrincipal(String principal) {
        return policy.get().principalRoles().getOrDefault(normalizePrincipal(principal), Set.of());
    }

    /**
     * Mapping-key form of a principal: an optional Kafka-style {@code User:} prefix is dropped
     * and a distinguished name is rewritten to RFC 2253 (so {@code "CN=a, O=b"} equals the
     * certificate's {@code "CN=a,O=b"}). Anything that is not a DN (a bare CN) is kept as is.
     */
    static String normalizePrincipal(String principal) {
        String p = principal.strip();
        if (p.startsWith("User:")) p = p.substring("User:".length()).strip();
        if (!p.contains("=")) return p;
        try {
            return new X500Principal(p).getName();
        } catch (IllegalArgumentException notADn) {
            return p;
        }
    }

    /** Identity → "user:<name>" (certificate subject per mode for mTLS) or "anonymous". */
    public static String principalOf(SecurityIdentity identity, MtlsPrincipal.Mode mode) {
        if (identity == null || identity.isAnonymous()) return "anonymous";
        if (MtlsPrincipal.isCertificateIdentity(identity)) {
            return "user:" + MtlsPrincipal.principalOf(identity, mode);
        }
        return identity.getPrincipal() != null && identity.getPrincipal().getName() != null
                ? "user:" + identity.getPrincipal().getName() : "anonymous";
    }

    /** Role-file rules for a set of roles (token roles, or roles mapped to a certificate). */
    public boolean isAllowed(Set<String> callerRoles, String artifact, Action action) {
        for (Rule rule : policy.get().rules()) {
            if (!Collections.disjoint(callerRoles, rule.roles())) {
                for (Resource res : rule.resources()) {
                    if (("*".equals(res.artifact()) || res.artifact().equals(artifact))
                            && res.actions().contains(action)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    void reload(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            Map<String, Object> root = new Yaml().load(in);
            List<Map<String, Object>> rawRules = (List<Map<String, Object>>) root.get("rules");
            List<Rule> parsed = new ArrayList<>();
            if (rawRules != null) {
                for (Map<String, Object> rawRule : rawRules) {
                    List<String> roles = toStringList(rawRule.get("roles"));
                    List<Resource> resources = new ArrayList<>();
                    List<Map<String, Object>> rawResources = (List<Map<String, Object>>) rawRule.get("resources");
                    if (rawResources != null) {
                        for (Map<String, Object> rawRes : rawResources) {
                            String artifact = (String) rawRes.get("artifact");
                            List<Action> actions = toStringList(rawRes.get("actions"))
                                .stream().map(Action::valueOf).toList();
                            resources.add(new Resource(artifact, actions));
                        }
                    }
                    parsed.add(new Rule(roles, resources));
                }
            }
            Map<String, Set<String>> principalRoles = parsePrincipals(root.get("principals"));
            policy.set(new Policy(List.copyOf(parsed), principalRoles));
            initialized = true;
            System.out.println("[PolicyEngine] Loaded " + parsed.size() + " rules and "
                    + principalRoles.size() + " principal mappings from " + path);
        }
    }

    /** {@code principals:} is a map of certificate principal → role name or list of roles. */
    private static Map<String, Set<String>> parsePrincipals(Object raw) {
        if (raw == null) return Map.of();
        if (!(raw instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("'principals' must be a map of principal -> roles");
        }
        Map<String, Set<String>> out = new HashMap<>();
        for (Map.Entry<?, ?> e : map.entrySet()) {
            String key = normalizePrincipal(String.valueOf(e.getKey()));
            out.computeIfAbsent(key, k -> new HashSet<>()).addAll(toStringList(e.getValue()));
        }
        out.replaceAll((k, v) -> Set.copyOf(v));
        return Map.copyOf(out);
    }

    private void watchLoop(Path policyFile) {
        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            policyFile.getParent().register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
            while (running) {
                WatchKey key = watcher.poll(1, TimeUnit.SECONDS);
                if (key == null) continue;
                boolean relevant = key.pollEvents().stream()
                    .anyMatch(e -> policyFile.getFileName().equals(e.context()));
                key.reset();
                if (relevant) {
                    try {
                        Thread.sleep(50);
                        reload(policyFile);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception ex) {
                        System.err.println("[PolicyEngine] Reload failed: " + ex.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            if (running) System.err.println("[PolicyEngine] Watch loop error: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> toStringList(Object obj) {
        if (obj instanceof Collection<?> c) return c.stream().map(Object::toString).toList();
        if (obj instanceof String s) return List.of(s);
        return List.of();
    }
}
