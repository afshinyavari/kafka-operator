package se.afshin.yavari.rbac;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class PolicyEngine {

    public enum Action { READ, WRITE, DELETE }

    record Resource(String artifact, List<Action> actions) {}
    record Rule(List<String> roles, List<Resource> resources) {}

    @ConfigProperty(name = "proxy.policy.file")
    String policyFilePath;

    private final AtomicReference<List<Rule>> rules = new AtomicReference<>(List.of());
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

    public boolean isAllowed(Set<String> callerRoles, String artifact, Action action) {
        for (Rule rule : rules.get()) {
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
            rules.set(parsed);
            initialized = true;
            System.out.println("[PolicyEngine] Loaded " + parsed.size() + " rules from " + path);
        }
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
