package se.afshin.yavari.kafka.editor.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLogTest {

    private final AuditLog audit = new AuditLog();
    private final ObjectMapper mapper = new ObjectMapper();

    @AfterEach
    void clearMdc() {
        MDC.remove("correlationId");
    }

    @Test
    void formatEntry_emitsFlatJson_withCoreFields() throws Exception {
        String line = audit.formatEntry("alice", "topic.create", "orders-v1",
                "success", Map.of("partitions", 6, "rf", 3));
        JsonNode n = mapper.readTree(line);

        assertThat(n.get("user").asText()).isEqualTo("alice");
        assertThat(n.get("action").asText()).isEqualTo("topic.create");
        assertThat(n.get("target").asText()).isEqualTo("orders-v1");
        assertThat(n.get("outcome").asText()).isEqualTo("success");
        assertThat(n.get("ts").asText()).isNotBlank();
        assertThat(n.get("details").get("partitions").asInt()).isEqualTo(6);
        assertThat(n.get("details").get("rf").asInt()).isEqualTo(3);
    }

    @Test
    void formatEntry_includesCorrelationIdFromMdc_whenSet() throws Exception {
        MDC.put("correlationId", "abc-123");

        String line = audit.formatEntry("bob", "topic.delete", "logs", "success", null);
        JsonNode n = mapper.readTree(line);

        assertThat(n.get("correlationId").asText()).isEqualTo("abc-123");
    }

    @Test
    void formatEntry_omitsDetails_whenEmpty() throws Exception {
        String line = audit.formatEntry("eve", "schema.delete", "user-events",
                "failure", Map.of());
        JsonNode n = mapper.readTree(line);

        assertThat(n.has("details")).isFalse();
    }

    @Test
    void formatEntry_anonymousFallback_forNullUser() throws Exception {
        String line = audit.formatEntry(null, "group.delete", "etl-group", "success", null);
        JsonNode n = mapper.readTree(line);

        assertThat(n.get("user").asText()).isEqualTo("anonymous");
    }

    @Test
    void failure_writesOutcomeFailure_withReason() throws Exception {
        String line = audit.formatEntry("carol", "topic.alter", "orders-v1",
                "failure", Map.of("reason", "AuthorizationException"));
        JsonNode n = mapper.readTree(line);

        assertThat(n.get("outcome").asText()).isEqualTo("failure");
        assertThat(n.get("details").get("reason").asText()).isEqualTo("AuthorizationException");
    }
}
