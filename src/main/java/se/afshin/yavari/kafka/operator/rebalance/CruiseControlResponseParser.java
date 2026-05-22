package se.afshin.yavari.kafka.operator.rebalance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Pure JSON parsing for Cruise Control responses — extracted from {@link HttpCruiseControlClient}
 * so it can be unit-tested against captured payloads without any HTTP.
 */
final class CruiseControlResponseParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CruiseControlResponseParser() {}

    /**
     * Flattens the {@code summary} object of a rebalance/add_broker/remove_broker response
     * into a string map (scalar fields only). Returns an empty map when there is no summary.
     */
    static Map<String, String> parseSummary(String jsonBody) {
        Map<String, String> out = new LinkedHashMap<>();
        if (jsonBody == null || jsonBody.isBlank()) return out;
        try {
            JsonNode root = MAPPER.readTree(jsonBody);
            JsonNode summary = root.path("summary");
            if (summary.isMissingNode() || !summary.isObject()) return out;
            summary.fields().forEachRemaining(e -> {
                JsonNode v = e.getValue();
                if (v.isValueNode()) {
                    out.put(e.getKey(), v.asText());
                }
            });
        } catch (Exception e) {
            // Malformed body — return whatever was collected; the caller treats an empty
            // summary as "not ready yet".
        }
        return out;
    }

    /**
     * Reads {@code ExecutorState.state} from a {@code GET /state} response. Cruise Control
     * reports {@code NO_TASK_IN_PROGRESS} when the executor is idle. A missing executor
     * substate is also treated as idle.
     */
    static boolean parseExecutorIdle(String jsonBody) {
        if (jsonBody == null || jsonBody.isBlank()) return true;
        try {
            JsonNode root = MAPPER.readTree(jsonBody);
            JsonNode state = root.path("ExecutorState").path("state");
            if (state.isMissingNode()) return true;
            return "NO_TASK_IN_PROGRESS".equals(state.asText());
        } catch (Exception e) {
            return false;
        }
    }

    /** Extracts a human-readable error message from an error response body. */
    static String parseErrorMessage(String jsonBody, int statusCode) {
        if (jsonBody == null || jsonBody.isBlank()) {
            return "HTTP " + statusCode;
        }
        try {
            JsonNode root = MAPPER.readTree(jsonBody);
            for (String field : new String[] {"errorMessage", "message", "stackTrace"}) {
                JsonNode n = root.path(field);
                if (n.isTextual() && !n.asText().isBlank()) {
                    return truncate(n.asText());
                }
            }
        } catch (Exception ignored) {
            // not JSON — fall through to the raw body
        }
        return truncate(jsonBody);
    }

    private static String truncate(String s) {
        String oneLine = s.replaceAll("\\s+", " ").trim();
        return oneLine.length() > 300 ? oneLine.substring(0, 300) + "…" : oneLine;
    }
}
