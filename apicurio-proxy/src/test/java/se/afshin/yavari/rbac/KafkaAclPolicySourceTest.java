package se.afshin.yavari.rbac;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.KafkaAclPolicySource.evaluate;
import static se.afshin.yavari.rbac.KafkaAclPolicySource.topicForArtifact;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

class KafkaAclPolicySourceTest {

    private static final List<String> SUFFIXES = List.of("-value", "-key");
    private static final String SVC = "CN=orders-service";

    private static AclBinding acl(String principal, ResourceType type, String name, PatternType pattern,
                                  AclOperation op, AclPermissionType perm) {
        return new AclBinding(new ResourcePattern(type, name, pattern),
                new AccessControlEntry("User:" + principal, "*", op, perm));
    }

    private static AclBinding allow(String name, PatternType pattern, AclOperation op) {
        return acl(SVC, ResourceType.TOPIC, name, pattern, op, AclPermissionType.ALLOW);
    }

    // ── artifact → topic ──────────────────────────────────────────────────────

    @Test
    void stripsValueAndKeySuffixes() {
        assertThat(topicForArtifact("orders-value", SUFFIXES)).isEqualTo("orders");
        assertThat(topicForArtifact("orders-key", SUFFIXES)).isEqualTo("orders");
    }

    @Test
    void unknownSuffixMapsToSameName() {
        assertThat(topicForArtifact("orders", SUFFIXES)).isEqualTo("orders");
        assertThat(topicForArtifact("orders-schema", SUFFIXES)).isEqualTo("orders-schema");
    }

    @Test
    void wildcardArtifactMapsToWildcardTopic() {
        assertThat(topicForArtifact("*", SUFFIXES)).isEqualTo("*");
        assertThat(topicForArtifact(null, SUFFIXES)).isEqualTo("*");
    }

    // ── operation table ───────────────────────────────────────────────────────

    @Test
    void writeOnTopicGrantsReadAndWrite() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.LITERAL, AclOperation.WRITE));
        assertThat(evaluate(acls, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isTrue();
        assertThat(evaluate(acls, SVC, "orders", DELETE)).isFalse();
    }

    @Test
    void readOrDescribeOnTopicGrantsReadOnly() {
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.READ)), SVC, "orders", READ)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DESCRIBE)), SVC, "orders", READ)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.READ)), SVC, "orders", WRITE)).isFalse();
    }

    @Test
    void deleteAndAllMapAsExpected() {
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DELETE)), SVC, "orders", DELETE)).isTrue();
        assertThat(evaluate(List.of(allow("orders", PatternType.LITERAL, AclOperation.DELETE)), SVC, "orders", READ)).isFalse();
        List<AclBinding> all = List.of(allow("orders", PatternType.LITERAL, AclOperation.ALL));
        assertThat(evaluate(all, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(all, SVC, "orders", WRITE)).isTrue();
        assertThat(evaluate(all, SVC, "orders", DELETE)).isTrue();
    }

    // ── pattern matching ──────────────────────────────────────────────────────

    @Test
    void prefixedPatternMatchesTopicsWithThatPrefix() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.PREFIXED, AclOperation.WRITE));
        assertThat(evaluate(acls, SVC, "orders.created", WRITE)).isTrue();
        assertThat(evaluate(acls, SVC, "payments", WRITE)).isFalse();
    }

    @Test
    void literalWildcardNameMatchesEveryTopicIncludingStar() {
        List<AclBinding> acls = List.of(allow("*", PatternType.LITERAL, AclOperation.ALL));
        assertThat(evaluate(acls, SVC, "anything", DELETE)).isTrue();
        assertThat(evaluate(acls, SVC, "*", READ)).isTrue();
    }

    @Test
    void starTopicRequiresWildcardAcl() {
        List<AclBinding> acls = List.of(allow("orders", PatternType.PREFIXED, AclOperation.ALL));
        assertThat(evaluate(acls, SVC, "*", READ)).isFalse();
    }

    @Test
    void wildcardPrincipalApplies() {
        List<AclBinding> acls = List.of(acl("*", ResourceType.TOPIC, "orders", PatternType.LITERAL,
                AclOperation.READ, AclPermissionType.ALLOW));
        assertThat(evaluate(acls, "CN=someone-else", "orders", READ)).isTrue();
    }

    @Test
    void otherPrincipalsAndResourceTypesAreIgnored() {
        List<AclBinding> acls = List.of(
                acl("CN=other", ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.ALLOW),
                acl(SVC, ResourceType.GROUP, "orders", PatternType.LITERAL, AclOperation.ALL, AclPermissionType.ALLOW));
        assertThat(evaluate(acls, SVC, "orders", READ)).isFalse();
    }

    // ── deny precedence ───────────────────────────────────────────────────────

    @Test
    void denyOnOperationBeatsAllow() {
        List<AclBinding> acls = List.of(
                allow("*", PatternType.LITERAL, AclOperation.ALL),
                acl(SVC, ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE, AclPermissionType.DENY));
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isFalse();
        // READ is still granted through ALL (the DENY targets WRITE only)
        assertThat(evaluate(acls, SVC, "orders", READ)).isTrue();
        assertThat(evaluate(acls, SVC, "payments", WRITE)).isTrue();
    }

    @Test
    void denyAllBeatsEverything() {
        List<AclBinding> acls = List.of(
                allow("orders", PatternType.LITERAL, AclOperation.WRITE),
                acl(SVC, ResourceType.TOPIC, "orders", PatternType.PREFIXED, AclOperation.ALL, AclPermissionType.DENY));
        assertThat(evaluate(acls, SVC, "orders", READ)).isFalse();
        assertThat(evaluate(acls, SVC, "orders", WRITE)).isFalse();
    }

    @Test
    void emptySnapshotDeniesEverything() {
        assertThat(evaluate(List.of(), SVC, "orders", READ)).isFalse();
    }
}
