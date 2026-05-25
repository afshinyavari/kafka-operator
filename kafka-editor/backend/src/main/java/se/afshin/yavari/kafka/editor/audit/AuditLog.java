package se.afshin.yavari.kafka.editor.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Structured audit log for write operations issued through the editor.
 *
 * Each entry is one JSON line at INFO level on the dedicated logger
 * {@code kafka-editor.audit}. The fields are intentionally flat so the line can be
 * picked up by a log aggregator (Loki / ELK) without an extra parser.
 *
 * Correlation id is read from MDC when present (the operator/UI pipeline
 * already populates it via the Wave 6 logging work).
 */
@ApplicationScoped
public class AuditLog {

    private static final Logger LOG = Logger.getLogger("kafka-editor.audit");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void success(String user, String action, String target, Map<String, Object> details) {
        LOG.info(formatEntry(user, action, target, "success", details));
    }

    public void failure(String user, String action, String target, String reason) {
        LOG.info(formatEntry(user, action, target, "failure", Map.of("reason", reason == null ? "" : reason)));
    }

    /** Returns a single JSON line. Exposed for tests. */
    public String formatEntry(String user, String action, String target,
                              String outcome, Map<String, Object> details) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("user", user == null ? "anonymous" : user);
        entry.put("action", action);
        entry.put("target", target);
        entry.put("outcome", outcome);
        Object corr = MDC.get("correlationId");
        if (corr != null) entry.put("correlationId", corr.toString());
        if (details != null && !details.isEmpty()) entry.put("details", details);
        try {
            return MAPPER.writeValueAsString(entry);
        } catch (JsonProcessingException e) {
            return "{\"audit_serialize_error\":\"" + e.getMessage() + "\"}";
        }
    }
}
