package se.afshin.yavari.kroxy.auth.rbac;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.plugin.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicReference;

@Plugin(configType = GroupAwareAuthorizerConfig.class)
public class GroupAwareAuthorizerService implements AuthorizerService<GroupAwareAuthorizerConfig> {

    private static final Logger log = LoggerFactory.getLogger(GroupAwareAuthorizerService.class);

    private final AtomicReference<RbacRules> rules = new AtomicReference<>(new RbacRules());
    private volatile boolean running = false;
    private Thread watchThread;
    private Path rulesPath;

    @Override
    public void initialize(GroupAwareAuthorizerConfig config) {
        rulesPath = Path.of(config.getRulesFile());
        rules.set(load(rulesPath));

        running = true;
        watchThread = Thread.ofVirtual().name("rbac-watcher").start(this::watchLoop);
    }

    @Override
    public Authorizer build() {
        return new GroupAwareAuthorizer(rules);
    }

    @Override
    public void close() {
        running = false;
        if (watchThread != null) {
            watchThread.interrupt();
        }
    }

    private void watchLoop() {
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            rulesPath.getParent().register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
            String filename = rulesPath.getFileName().toString();

            while (running) {
                WatchKey key = watcher.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.context().toString().equals(filename)) {
                        rules.set(load(rulesPath));
                        log.info("RBAC rules reloaded from {}", rulesPath);
                    }
                }
                key.reset();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            log.error("Failed to watch RBAC rules file {}", rulesPath, e);
        }
    }

    private static RbacRules load(Path path) {
        try (FileReader reader = new FileReader(path.toFile())) {
            Yaml yaml = new Yaml();
            RbacRules loaded = yaml.loadAs(reader, RbacRules.class);
            return loaded != null ? loaded : new RbacRules();
        } catch (IOException e) {
            log.error("Failed to load RBAC rules from {}", path, e);
            return new RbacRules();
        }
    }
}
