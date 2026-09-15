package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** In-process Confluent Schema Registry stub covering the endpoints the SMT calls.
 *  Schemas are keyed by id; each subject holds an ordered list of versions. */
public class FakeConfluent implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Schema(int id, String subject, int version, String type, String schema,
                         List<Ref> references) {}
    public record Ref(String name, String subject, int version) {}

    final Map<Integer, Schema> byId = new LinkedHashMap<>();
    final Map<String, List<Schema>> bySubject = new LinkedHashMap<>();
    int nextId = 1;
    final AtomicInteger registerCalls = new AtomicInteger();
    final AtomicInteger fetchCalls = new AtomicInteger();
    /** When set, requests lacking this exact Authorization header get 401. */
    volatile String requireAuthorization;

    /** Registers a schema under {@code subject} and returns its id. Identical content
     *  under the same subject returns the existing id (Confluent semantics). */
    public synchronized Schema register(String subject, String type, String schema, List<Ref> refs) {
        List<Schema> versions = bySubject.computeIfAbsent(subject, s -> new ArrayList<>());
        for (Schema s : versions) {
            if (s.schema().equals(schema)) return s;
        }
        Schema s = new Schema(nextId++, subject, versions.size() + 1, type, schema, refs);
        versions.add(s);
        byId.put(s.id(), s);
        return s;
    }

    public void assignNextId(int id) { nextId = id; }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            if (requireAuthorization != null
                    && !requireAuthorization.equals(ex.getRequestHeaders().getFirst("Authorization"))) {
                ex.sendResponseHeaders(401, -1);
                return;
            }
            // GET /schemas/ids/{id}
            if (method.equals("GET") && path.matches("^/schemas/ids/\\d+$")) {
                fetchCalls.incrementAndGet();
                Schema s = byId.get(lastInt(path));
                if (s == null) { sendJson(ex, 404, "{\"error_code\":40403,\"message\":\"Schema not found\"}"); return; }
                ObjectNode n = MAPPER.createObjectNode();
                n.put("schema", s.schema());
                if (!"AVRO".equals(s.type())) n.put("schemaType", s.type());
                if (!s.references().isEmpty()) writeRefs(n.putArray("references"), s.references());
                sendJson(ex, 200, MAPPER.writeValueAsString(n));
                return;
            }
            // GET /schemas/ids/{id}/versions
            if (method.equals("GET") && path.matches("^/schemas/ids/\\d+/versions$")) {
                Schema s = byId.get(Integer.parseInt(path.split("/")[3]));
                if (s == null) { sendJson(ex, 404, "{\"error_code\":40403}"); return; }
                sendJson(ex, 200, "[{\"subject\":\"" + s.subject() + "\",\"version\":" + s.version() + "}]");
                return;
            }
            // GET /subjects/{subject}/versions/{version}
            if (method.equals("GET") && path.matches("^/subjects/[^/]+/versions/[^/]+$")) {
                String[] parts = path.split("/");
                List<Schema> versions = bySubject.get(parts[2]);
                if (versions == null) { sendJson(ex, 404, "{\"error_code\":40401}"); return; }
                Schema s = "latest".equals(parts[4]) ? versions.get(versions.size() - 1)
                        : versions.stream().filter(v -> v.version() == Integer.parseInt(parts[4])).findFirst().orElse(null);
                if (s == null) { sendJson(ex, 404, "{\"error_code\":40402}"); return; }
                sendJson(ex, 200, MAPPER.writeValueAsString(Map.of(
                        "subject", s.subject(), "version", s.version(), "id", s.id(), "schema", s.schema())));
                return;
            }
            // POST /subjects/{subject}/versions — register
            if (method.equals("POST") && path.matches("^/subjects/[^/]+/versions$")) {
                registerCalls.incrementAndGet();
                JsonNode body = MAPPER.readTree(ex.getRequestBody().readAllBytes());
                List<Ref> refs = new ArrayList<>();
                if (body.has("references")) {
                    for (JsonNode r : body.get("references")) {
                        refs.add(new Ref(r.get("name").asText(), r.get("subject").asText(), r.get("version").asInt()));
                    }
                }
                Schema s = register(path.split("/")[2],
                        body.has("schemaType") ? body.get("schemaType").asText() : "AVRO",
                        body.get("schema").asText(), refs);
                sendJson(ex, 200, "{\"id\":" + s.id() + "}");
                return;
            }
            // POST /subjects/{subject} — lookup by content
            if (method.equals("POST") && path.matches("^/subjects/[^/]+$")) {
                JsonNode body = MAPPER.readTree(ex.getRequestBody().readAllBytes());
                List<Schema> versions = bySubject.getOrDefault(path.split("/")[2], List.of());
                Schema s = versions.stream().filter(v -> v.schema().equals(body.get("schema").asText()))
                        .findFirst().orElse(null);
                if (s == null) { sendJson(ex, 404, "{\"error_code\":40403}"); return; }
                sendJson(ex, 200, MAPPER.writeValueAsString(Map.of(
                        "subject", s.subject(), "version", s.version(), "id", s.id(), "schema", s.schema())));
                return;
            }
            ex.sendResponseHeaders(404, -1);
        } finally {
            ex.close();
        }
    }

    private static void writeRefs(ArrayNode arr, List<Ref> refs) {
        for (Ref r : refs) {
            ObjectNode o = arr.addObject();
            o.put("name", r.name());
            o.put("subject", r.subject());
            o.put("version", r.version());
        }
    }

    private static int lastInt(String path) {
        String[] parts = path.split("/");
        return Integer.parseInt(parts[parts.length - 1]);
    }

    static void sendJson(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }
}
