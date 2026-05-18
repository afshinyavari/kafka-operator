package se.afshin.yavari.rbac;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

class PolicyEngineTest {

    private PolicyEngine engine;
    private Path policyFile;

    @BeforeEach
    void setUp() throws Exception {
        engine = new PolicyEngine();
        policyFile = Files.createTempFile("test-policy", ".yaml");
    }

    @AfterEach
    void tearDown() throws Exception {
        Files.deleteIfExists(policyFile);
    }

    private void load(String yaml) throws Exception {
        Files.writeString(policyFile, yaml);
        engine.reload(policyFile);
    }

    @Test
    void specificRoleAllowsMatchingArtifactAndActions() throws Exception {
        load("""
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ, WRITE]
            """);

        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", READ)).isTrue();
        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", WRITE)).isTrue();
        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", DELETE)).isFalse();
    }

    @Test
    void specificRoleDeniesOtherArtifact() throws Exception {
        load("""
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ, WRITE]
            """);

        assertThat(engine.isAllowed(Set.of("orders-team"), "invoices", READ)).isFalse();
        assertThat(engine.isAllowed(Set.of("other-role"), "orders", READ)).isFalse();
    }

    @Test
    void wildcardArtifactMatchesAny() throws Exception {
        load("""
            rules:
              - roles: [schema-admin]
                resources:
                  - artifact: "*"
                    actions: [READ, WRITE, DELETE]
            """);

        assertThat(engine.isAllowed(Set.of("schema-admin"), "orders", READ)).isTrue();
        assertThat(engine.isAllowed(Set.of("schema-admin"), "invoices", DELETE)).isTrue();
        assertThat(engine.isAllowed(Set.of("schema-admin"), "anything", WRITE)).isTrue();
        assertThat(engine.isAllowed(Set.of("other-role"), "orders", READ)).isFalse();
    }

    @Test
    void callerWithMultipleRolesGrantedIfAnyMatches() throws Exception {
        load("""
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ]
            """);

        assertThat(engine.isAllowed(Set.of("orders-team", "unrelated-role"), "orders", READ)).isTrue();
    }

    @Test
    void emptyRulesDenyEverything() throws Exception {
        load("rules: []");
        assertThat(engine.isAllowed(Set.of("any-role"), "any-artifact", READ)).isFalse();
    }

    @Test
    void multipleRulesEachApplied() throws Exception {
        load("""
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ, WRITE]
              - roles: [invoices-team]
                resources:
                  - artifact: invoices
                    actions: [READ, WRITE]
            """);

        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", READ)).isTrue();
        assertThat(engine.isAllowed(Set.of("invoices-team"), "invoices", READ)).isTrue();
        assertThat(engine.isAllowed(Set.of("orders-team"), "invoices", READ)).isFalse();
        assertThat(engine.isAllowed(Set.of("invoices-team"), "orders", READ)).isFalse();
    }

    @Test
    void reloadReplacesRules() throws Exception {
        load("""
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders
                    actions: [READ]
            """);
        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", READ)).isTrue();

        // Reload with stricter policy: orders-team loses access
        load("rules: []");
        assertThat(engine.isAllowed(Set.of("orders-team"), "orders", READ)).isFalse();
    }
}
