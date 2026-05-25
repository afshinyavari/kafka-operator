package se.afshin.yavari.kroxy.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/** Single-line JSON serialiser for {@link AuditEvent}. */
final class AuditEventJson {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AuditEventJson() {}

    static String toJson(AuditEvent event) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", event.ts() == null ? null : event.ts().toString());
        entry.put("principal", event.principal() == null ? "anonymous" : event.principal());
        entry.put("op", event.op());
        entry.put("resource", event.resource());
        entry.put("decision", event.decision());
        entry.put("latencyMs", event.latencyMs());
        if (event.correlationId() != null) {
            entry.put("correlationId", event.correlationId());
        }
        try {
            return MAPPER.writeValueAsString(entry);
        } catch (JsonProcessingException e) {
            return "{\"audit_serialize_error\":\"" + e.getMessage() + "\"}";
        }
    }
}
