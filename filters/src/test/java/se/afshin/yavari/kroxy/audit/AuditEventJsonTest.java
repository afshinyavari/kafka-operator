package se.afshin.yavari.kroxy.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AuditEventJsonTest {

    @Test
    void allFieldsPresent_inOrder() {
        AuditEvent e = new AuditEvent(
                Instant.parse("2026-05-25T12:00:00Z"),
                "user:alice", "PRODUCE", "orders", "allow", 12, "abc-123");

        String json = AuditEventJson.toJson(e);

        // Field order — the JSON is consumed by log shippers that key off it being stable.
        assertThat(json).isEqualTo(
                "{\"ts\":\"2026-05-25T12:00:00Z\",\"principal\":\"user:alice\",\"op\":\"PRODUCE\","
                        + "\"resource\":\"orders\",\"decision\":\"allow\",\"latencyMs\":12,"
                        + "\"correlationId\":\"abc-123\"}");
    }

    @Test
    void nullCorrelationId_omitted() {
        AuditEvent e = new AuditEvent(
                Instant.parse("2026-05-25T12:00:00Z"),
                "user:alice", "FETCH", "events", "deny", 3, null);

        String json = AuditEventJson.toJson(e);

        assertThat(json).doesNotContain("correlationId");
        assertThat(json).contains("\"decision\":\"deny\"");
    }

    @Test
    void nullPrincipal_renderedAsAnonymous() {
        AuditEvent e = new AuditEvent(
                Instant.parse("2026-05-25T12:00:00Z"),
                null, "FETCH", "*", "allow", 1, null);

        String json = AuditEventJson.toJson(e);

        assertThat(json).contains("\"principal\":\"anonymous\"");
    }

    @Test
    void latencyZero_serialisesAsNumber() {
        AuditEvent e = new AuditEvent(
                Instant.parse("2026-05-25T12:00:00Z"),
                "user:alice", "PRODUCE", "x", "allow", 0, null);

        String json = AuditEventJson.toJson(e);

        assertThat(json).contains("\"latencyMs\":0");
        assertThat(json).doesNotContain("\"latencyMs\":\"0\"");
    }
}
