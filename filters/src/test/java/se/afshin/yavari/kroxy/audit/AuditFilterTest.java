package se.afshin.yavari.kroxy.audit;

import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AuditFilterTest {

    @Test
    void authFailureErrorCodes_areAllRecognised() {
        // Every authz-failure error code Kroxylicious can surface via the authorization
        // filter's short-circuit response. Audit relies on these to set decision=deny.
        assertThat(AuditFilter.isAuthFailure(Errors.TOPIC_AUTHORIZATION_FAILED.code())).isTrue();
        assertThat(AuditFilter.isAuthFailure(Errors.CLUSTER_AUTHORIZATION_FAILED.code())).isTrue();
        assertThat(AuditFilter.isAuthFailure(Errors.GROUP_AUTHORIZATION_FAILED.code())).isTrue();
        assertThat(AuditFilter.isAuthFailure(
                Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code())).isTrue();
        assertThat(AuditFilter.isAuthFailure(
                Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED.code())).isTrue();
    }

    @Test
    void nonAuthFailures_areNotDecisions() {
        assertThat(AuditFilter.isAuthFailure(Errors.NONE.code())).isFalse();
        assertThat(AuditFilter.isAuthFailure(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())).isFalse();
        assertThat(AuditFilter.isAuthFailure(Errors.NOT_LEADER_OR_FOLLOWER.code())).isFalse();
        assertThat(AuditFilter.isAuthFailure(Errors.INVALID_RECORD.code())).isFalse();
    }

    @Test
    void constructor_acceptsNullIncludeOps() {
        // Null collapses to "emit everything" — the filter's emit() guard short-circuits.
        new AuditFilter(new StdoutAuditEmitter(), null);
        new AuditFilter(new StdoutAuditEmitter(), Set.of("WRITE"));
    }
}
