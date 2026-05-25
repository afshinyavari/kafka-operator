package se.afshin.yavari.kroxy.auth.rbac;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class GroupAwareAuthorizerTest {

    private static final Subject ALICE = new Subject(new User("alice"));

    /** Test-local stand-in for Kroxylicious's {@code TopicResource} enum — the real one
     *  lives in {@code kroxylicious-authorization} (runtime classpath in the proxy, not
     *  on the filters module's compile classpath). All we need is something with the
     *  same names so {@link GroupAwareAuthorizer}'s {@code operationName(Action)} helper
     *  yields the matching string. */
    enum Op implements ResourceType<Op> {
        READ, WRITE, DESCRIBE, CREATE, DELETE, ALTER, DESCRIBE_CONFIGS, ALTER_CONFIGS
    }

    @Test
    void grantingWrite_impliesDescribe() {
        // A producer's first call is METADATA, which Kroxylicious authorises as
        // DESCRIBE on the topic. Our rule lists only WRITE — but Kafka's
        // StandardAuthorizer (and now ours) treats READ/WRITE/DELETE/ALTER as
        // implying DESCRIBE, so METADATA must still succeed.
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("orders"), List.of("WRITE"));

        assertThat(allowed(authz, Op.WRITE, "orders")).isTrue();
        assertThat(allowed(authz, Op.DESCRIBE, "orders")).isTrue();
        assertThat(allowed(authz, Op.READ, "orders")).isFalse();
    }

    @Test
    void grantingRead_impliesDescribe() {
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("orders"), List.of("READ"));

        assertThat(allowed(authz, Op.READ, "orders")).isTrue();
        assertThat(allowed(authz, Op.DESCRIBE, "orders")).isTrue();
        assertThat(allowed(authz, Op.WRITE, "orders")).isFalse();
    }

    @Test
    void grantingDelete_impliesDescribe() {
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("orders"), List.of("DELETE"));

        assertThat(allowed(authz, Op.DELETE, "orders")).isTrue();
        assertThat(allowed(authz, Op.DESCRIBE, "orders")).isTrue();
    }

    @Test
    void grantingAlterConfigs_impliesDescribeConfigs() {
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("orders"), List.of("ALTER_CONFIGS"));

        assertThat(allowed(authz, Op.ALTER_CONFIGS, "orders")).isTrue();
        assertThat(allowed(authz, Op.DESCRIBE_CONFIGS, "orders")).isTrue();
        // ALTER_CONFIGS also grants DESCRIBE on the topic — same StandardAuthorizer rule.
        assertThat(allowed(authz, Op.DESCRIBE, "orders")).isTrue();
    }

    @Test
    void wildcardOperation_grantsEverything() {
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("*"), List.of("*"));

        assertThat(allowed(authz, Op.READ, "anything")).isTrue();
        assertThat(allowed(authz, Op.WRITE, "anything")).isTrue();
        assertThat(allowed(authz, Op.CREATE, "anything")).isTrue();
    }

    @Test
    void topicMismatch_denied() {
        GroupAwareAuthorizer authz = authorizerForUser("alice", List.of("orders"), List.of("WRITE"));

        assertThat(allowed(authz, Op.WRITE, "invoices")).isFalse();
    }

    @Test
    void unmatchedUser_denied() {
        GroupAwareAuthorizer authz = authorizerForUser("bob", List.of("orders"), List.of("WRITE"));

        assertThat(allowed(authz, Op.WRITE, "orders")).isFalse();
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static GroupAwareAuthorizer authorizerForUser(String userName,
                                                          List<String> topics,
                                                          List<String> operations) {
        RbacUser user = new RbacUser();
        user.name = userName;
        user.topics = topics;
        user.operations = operations;
        RbacRules rules = new RbacRules();
        rules.users = List.of(user);
        return new GroupAwareAuthorizer(new AtomicReference<>(rules));
    }

    private static boolean allowed(GroupAwareAuthorizer authz, Op op, String topic) {
        Action action = new Action(op, topic);
        AuthorizeResult result = authz.authorize(ALICE, List.of(action)).toCompletableFuture().join();
        return result.allowed().contains(action);
    }
}
