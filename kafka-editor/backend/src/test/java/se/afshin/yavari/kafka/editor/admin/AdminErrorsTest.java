package se.afshin.yavari.kafka.editor.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

/** Pure unit tests for Kafka-exception -> HTTP-status translation. */
class AdminErrorsTest {

    @Test
    void topicExistsMapsToConflict() {
        AdminApiException e = AdminErrors.translate(new TopicExistsException("dup"));
        assertEquals(409, e.status());
        assertEquals("CONFLICT", e.kind());
    }

    @Test
    void unknownTopicMapsToNotFound() {
        AdminApiException e =
                AdminErrors.translate(new UnknownTopicOrPartitionException("gone"));
        assertEquals(404, e.status());
        assertEquals("NOT_FOUND", e.kind());
    }

    @Test
    void securityDisabledMapsToUnsupported() {
        AdminApiException e =
                AdminErrors.translate(new SecurityDisabledException("no authorizer"));
        assertEquals(400, e.status());
        assertEquals("UNSUPPORTED", e.kind());
    }

    @Test
    void unknownErrorMapsToInternal() {
        AdminApiException e = AdminErrors.translate(new RuntimeException("boom"));
        assertEquals(500, e.status());
        assertEquals("INTERNAL", e.kind());
    }

    @Test
    void existingAdminApiExceptionPassesThrough() {
        AdminApiException original = new AdminApiException(404, "NOT_FOUND", "x");
        assertSame(original, AdminErrors.translate(original));
    }
}
