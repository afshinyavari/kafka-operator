package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** In-process Apicurio <strong>v2</strong> stub. Implements just the endpoints the SMT calls.
 *  Artifacts are keyed by globalId; versions are numbered per group/artifactId. */
public class FakeApicurio implements HttpHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Artifact(String groupId, String artifactId, String type, String content,
                           String version, List<SchemaRef> references) {}

    final Map<Long, Artifact> byGlobalId = new LinkedHashMap<>();
    /** group/artifactId → globalIds in creation order (index + 1 = version). */
    final Map<String, List<Long>> versions = new HashMap<>();
    long nextAssignedGlobalId = 1L;
    /** Counts content lookups (GET /ids/globalIds/{id}). */
    final AtomicInteger metadataCalls = new AtomicInteger();
    final AtomicInteger upsertCalls = new AtomicInteger();
    /** Upserts in the order received. */
    final List<Artifact> upserts = new ArrayList<>();
    /** When set, any request lacking {@code Authorization: Bearer <requireBearer>} gets 401. */
    volatile String requireBearer;
    /** Number of upcoming POSTs to reject with 401 (simulates a stale token). */
    final AtomicInteger rejectPostsRemaining = new AtomicInteger();
    /** When > 0, the next N requests (any method) fail with {@link #failStatus}. */
    final AtomicInteger failNextRequests = new AtomicInteger();
    volatile int failStatus = 503;

    public void register(long globalId, String groupId, String artifactId, String type, String content) {
        register(globalId, groupId, artifactId, type, content, List.of());
    }

    public synchronized void register(long globalId, String groupId, String artifactId, String type,
                                      String content, List<SchemaRef> refs) {
        List<Long> v = versions.computeIfAbsent(groupId + "/" + artifactId, k -> new ArrayList<>());
        v.add(globalId);
        byGlobalId.put(globalId, new Artifact(groupId, artifactId, type, content,
                String.valueOf(v.size()), refs));
    }

    public void assignNextGlobalId(long id) { nextAssignedGlobalId = id; }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            String query = ex.getRequestURI().getQuery();
            String method = ex.getRequestMethod();
            if (requireBearer != null
                    && !("Bearer " + requireBearer).equals(ex.getRequestHeaders().getFirst("Authorization"))) {
                ex.sendResponseHeaders(401, -1);
                return;
            }
            if (failNextRequests.getAndUpdate(x -> x > 0 ? x - 1 : 0) > 0) {
                sendJson(ex, failStatus, "{\"message\":\"injected failure\"}");
                return;
            }
            // GET /apis/registry/v2/ids/globalIds/{id} — raw schema content.
            if (method.equals("GET") && path.matches("^/apis/registry/v2/ids/globalIds/\\d+$")) {
                metadataCalls.incrementAndGet();
                Artifact a = byGlobalId.get(idFromPath(path));
                if (a == null) { ex.sendResponseHeaders(404, -1); return; }
                sendBytes(ex, 200, a.content().getBytes(StandardCharsets.UTF_8));
                return;
            }
            // GET /apis/registry/v2/ids/globalIds/{id}/references — references list.
            if (method.equals("GET") && path.matches("^/apis/registry/v2/ids/globalIds/\\d+/references$")) {
                Artifact a = byGlobalId.get(Long.parseLong(path.split("/")[6]));
                List<Map<String, String>> refs = new ArrayList<>();
                if (a != null) {
                    for (SchemaRef r : a.references()) {
                        refs.add(Map.of("name", r.name(), "groupId", r.groupId(),
                                "artifactId", r.artifactId(), "version", r.version()));
                    }
                }
                sendJson(ex, 200, MAPPER.writeValueAsString(refs));
                return;
            }
            // GET /apis/registry/v2/search/artifacts?globalId={id} — artifact identity.
            if (method.equals("GET") && path.equals("/apis/registry/v2/search/artifacts")) {
                long id = Long.parseLong((query == null ? "" : query).replaceAll(".*globalId=(\\d+).*", "$1"));
                Artifact a = byGlobalId.get(id);
                sendJson(ex, 200, a == null
                        ? "{\"artifacts\":[],\"count\":0}"
                        : MAPPER.writeValueAsString(Map.of(
                                "artifacts", List.of(Map.of("id", a.artifactId(), "type", a.type())),
                                "count", 1)));
                return;
            }
            // GET /apis/registry/v2/groups/{g}/artifacts/{a}/versions/{v}/meta and .../artifacts/{a}/meta
            if (method.equals("GET") && path.matches(
                    "^/apis/registry/v2/groups/[^/]+/artifacts/[^/]+(/versions/[^/]+)?/meta$")) {
                String[] parts = path.split("/");
                List<Long> v = versions.get(parts[5] + "/" + parts[7]);
                if (v == null || v.isEmpty()) { ex.sendResponseHeaders(404, -1); return; }
                Long gid;
                if (parts.length == 11) {
                    int idx = Integer.parseInt(parts[9]) - 1;
                    gid = idx >= 0 && idx < v.size() ? v.get(idx) : null;
                } else {
                    gid = v.get(v.size() - 1);
                }
                if (gid == null) { ex.sendResponseHeaders(404, -1); return; }
                // Tolerate an artifact whose content was removed (tests simulate a
                // registry whose metadata still lists a vanished version).
                Artifact meta = byGlobalId.get(gid);
                String version = meta != null ? meta.version() : String.valueOf(v.indexOf(gid) + 1);
                sendJson(ex, 200, "{\"globalId\":" + gid + ",\"version\":\"" + version + "\"}");
                return;
            }
            // POST /apis/registry/v2/groups/{g}/artifacts — create; returns ArtifactMetaData.
            if (method.equals("POST") && path.matches("^/apis/registry/v2/groups/[^/]+/artifacts$")) {
                if (rejectPostsRemaining.getAndUpdate(x -> x > 0 ? x - 1 : 0) > 0) {
                    ex.sendResponseHeaders(401, -1);
                    return;
                }
                upsertCalls.incrementAndGet();
                String groupId = path.split("/")[5];
                String artifactId = ex.getRequestHeaders().getFirst("X-Registry-ArtifactId");
                String type = ex.getRequestHeaders().getFirst("X-Registry-ArtifactType");
                byte[] body = ex.getRequestBody().readAllBytes();
                String content;
                List<SchemaRef> refs = new ArrayList<>();
                if ("application/create.extended+json".equals(ex.getRequestHeaders().getFirst("Content-Type"))) {
                    JsonNode env = MAPPER.readTree(body);
                    content = env.get("content").asText();
                    for (JsonNode r : env.get("references")) {
                        refs.add(new SchemaRef(text(r, "name"), text(r, "groupId"),
                                text(r, "artifactId"), text(r, "version")));
                    }
                } else {
                    content = new String(body, StandardCharsets.UTF_8);
                }
                // RETURN_OR_UPDATE: identical content under the same artifact returns the existing version.
                long assigned = -1;
                for (Long gid : versions.getOrDefault(groupId + "/" + artifactId, List.of())) {
                    if (byGlobalId.get(gid).content().equals(content)) { assigned = gid; break; }
                }
                if (assigned < 0) {
                    assigned = nextAssignedGlobalId++;
                    register(assigned, groupId, artifactId, type, content, refs);
                }
                Artifact stored = byGlobalId.get(assigned);
                upserts.add(stored);
                sendJson(ex, 200, MAPPER.writeValueAsString(Map.of(
                        "globalId", assigned, "version", stored.version(), "id", artifactId)));
                return;
            }
            ex.sendResponseHeaders(404, -1);
        } finally {
            ex.close();
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static long idFromPath(String path) {
        String[] parts = path.split("/");
        return Long.parseLong(parts[parts.length - 1]);
    }

    static void sendJson(HttpExchange ex, int code, String body) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "application/json");
        sendBytes(ex, code, body.getBytes(StandardCharsets.UTF_8));
    }

    static void sendBytes(HttpExchange ex, int code, byte[] body) throws IOException {
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }
}
