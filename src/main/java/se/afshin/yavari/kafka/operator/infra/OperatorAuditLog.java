package se.afshin.yavari.kafka.operator.infra;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Emits one-line JSON audit events on the {@code kafka-audit} SLF4J channel — the same
 * sink the in-process {@link se.afshin.yavari.kafka.operator.audit.AuditOrchestrator}-
 * configured Kroxylicious AuditFilter and the Apicurio rbac-proxy write to. Lets reconcilers
 * for admin operations (backup, restore, validation) participate in the unified audit pipeline
 * without depending on the filters module.
 *
 * <p>Field shape mirrors {@code filters/.../AuditEventJson} (ts/principal/op/resource/
 * decision/correlationId) so downstream log shippers can route operator-emitted events
 * through the same channel without per-source parsing.
 */
public final class OperatorAuditLog {

    private static final Logger LOG = Logger.getLogger("kafka-audit");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** The operator runs as its own ServiceAccount; events are attributed to that principal. */
    private static final String OPERATOR_PRINCIPAL = "system:operator";

    private OperatorAuditLog() {}

    /** Emit an audit event for an operator-driven action. {@code decision} is typically
     *  {@code "START"}, {@code "ALLOWED"} (success), or {@code "DENIED"} (failure). */
    public static void emit(String op, String resource, String decision, String correlationId) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("principal", OPERATOR_PRINCIPAL);
        entry.put("op", op);
        entry.put("resource", resource);
        entry.put("decision", decision);
        if (correlationId != null) {
            entry.put("correlationId", correlationId);
        }
        try {
            LOG.info(MAPPER.writeValueAsString(entry));
        } catch (JsonProcessingException e) {
            LOG.errorf("audit_serialize_error: %s", e.getMessage());
        }
    }
}
