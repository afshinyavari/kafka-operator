package se.afshin.yavari.rbac;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static se.afshin.yavari.rbac.PolicyEngine.Action.READ;

/** The engine picks up a changed policy file while running, laid out as a ConfigMap volume. */
class PolicyEngineHotReloadTest {

    private static final String ORDERS_READ = """
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ]
            """;

    @TempDir
    Path dir;

    private final PolicyEngine engine = new PolicyEngine();

    @AfterEach
    void tearDown() {
        engine.onStop();
    }

    private Path configMapVolume(String yaml) throws Exception {
        publish("..rev1", yaml);
        Files.createSymbolicLink(dir.resolve("..data"), Path.of("..rev1"));
        return Files.createSymbolicLink(dir.resolve("policy.yaml"), Path.of("..data", "policy.yaml"));
    }

    private void publish(String revision, String yaml) throws Exception {
        Path revisionDir = Files.createDirectory(dir.resolve(revision));
        Files.writeString(revisionDir.resolve("policy.yaml"), yaml);
    }

    private void updateConfigMap(String revision, String yaml) throws Exception {
        publish(revision, yaml);
        Path tmp = Files.createSymbolicLink(dir.resolve("..data_tmp"), Path.of(revision));
        Files.move(tmp, dir.resolve("..data"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private void start(Path policyFile) throws Exception {
        engine.policyFilePath = policyFile.toString();
        engine.reloadSeconds = 1;
        engine.onStart(null);
    }

    private boolean ordersTeamMayRead() {
        return engine.isAllowed(Set.of("orders-team"), "orders", READ);
    }

    @Test
    void configMapUpdateIsAppliedWithoutRestart() throws Exception {
        start(configMapVolume(ORDERS_READ));
        assertThat(ordersTeamMayRead()).isTrue();

        updateConfigMap("..rev2", "rules: []");

        long deadline = System.nanoTime() + 10_000_000_000L;
        while (ordersTeamMayRead() && System.nanoTime() < deadline) Thread.sleep(50);
        assertThat(ordersTeamMayRead()).isFalse();
    }

    @Test
    void brokenPolicyAtStartupFailsStartup() throws Exception {
        Path file = Files.writeString(dir.resolve("policy.yaml"), "rules: [not-a-rule");

        assertThatThrownBy(() -> start(file)).isInstanceOf(Exception.class);
        assertThat(engine.isInitialized()).isFalse();
    }
}
