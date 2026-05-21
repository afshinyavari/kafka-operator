package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Minimal Apicurio v3 REST client used by {@link ApicurioSchemaTransferSmt}. Avoids the
 * apicurio-registry-client SDK to keep the SMT JAR small and free of classloader
 * conflicts with Connect's own classpath.
 *
 * <p>Used endpoints:
 * <ul>
 *   <li>{@code GET /apis/registry/v3/ids/globalIds/{id}} — fetch raw schema content
 *   <li>{@code GET /apis/registry/v3/ids/globalIds/{id}/references} — fetch refs
 *   <li>{@code GET /apis/registry/v3/groups/{g}/artifacts/{a}/versions/latest} — find existing
 *   <li>{@code POST /apis/registry/v3/groups/{g}/artifacts?ifExists=RETURN_OR_UPDATE} — upsert
 * </ul>
 */
public class ApicurioClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient http;
    private final String authHeader;

    /** Auth modes carry the value already prefixed (e.g. "Basic ...", "Bearer ..."). */
    public ApicurioClient(String baseUrl, String authHeader) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.authHeader = authHeader;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public static String basicAuth(String username, String password) {
        if (username == null || password == null) return null;
        return "Basic " + Base64.getEncoder().encodeToString(
                (username + ":" + password).getBytes());
    }

    public static String bearerAuth(String token) {
        if (token == null) return null;
        return "Bearer " + token;
    }

    /** Result of resolving a globalId: content + parsed references + identity metadata. */
    public static class ArtifactByGlobalId {
        public final byte[] content;
        public final String groupId;
        public final String artifactId;
        public final String artifactType;
        public final List<ArtifactReference> references;

        ArtifactByGlobalId(byte[] content, String groupId, String artifactId,
                            String artifactType, List<ArtifactReference> references) {
            this.content = content;
            this.groupId = groupId;
            this.artifactId = artifactId;
            this.artifactType = artifactType;
            this.references = references;
        }
    }

    public record ArtifactReference(String name, String groupId, String artifactId, String version) {}

    /** Fetches the artifact metadata + content for a given globalId from this registry. */
    public ArtifactByGlobalId fetchByGlobalId(long globalId) throws ApicurioException {
        // Metadata first — we need groupId/artifactId/type to recreate on target.
        JsonNode meta = getJson("/apis/registry/v3/ids/globalIds/" + globalId);
        String groupId = textOrDefault(meta.get("groupId"), "default");
        String artifactId = meta.get("artifactId").asText();
        String artifactType = meta.get("artifactType").asText();
        // Content via the content endpoint.
        byte[] content = getRaw("/apis/registry/v3/ids/globalIds/" + globalId);
        // References.
        List<ArtifactReference> refs = new ArrayList<>();
        JsonNode refNode = getJsonOrNull("/apis/registry/v3/ids/globalIds/" + globalId + "/references");
        if (refNode != null && refNode.isArray()) {
            for (JsonNode r : refNode) {
                refs.add(new ArtifactReference(
                        textOrNull(r.get("name")),
                        textOrDefault(r.get("groupId"), "default"),
                        textOrNull(r.get("artifactId")),
                        textOrNull(r.get("version"))));
            }
        }
        return new ArtifactByGlobalId(content, groupId, artifactId, artifactType, refs);
    }

    /** Creates (or returns existing identical) artifact in this registry. Returns the
     *  globalId assigned by this registry. */
    public long upsertArtifact(String groupId, String artifactId, String artifactType,
                                byte[] content, List<ArtifactReference> references)
            throws ApicurioException {
        ObjectNode body = MAPPER.createObjectNode();
        body.put("artifactId", artifactId);
        body.put("artifactType", artifactType);
        ObjectNode firstVersion = body.putObject("firstVersion");
        ObjectNode contentNode = firstVersion.putObject("content");
        // Send raw content as a string; Apicurio interprets per artifactType (JSON for AVRO/JSON,
        // raw text for PROTOBUF/XML).
        contentNode.put("content", new String(content));
        if (references != null && !references.isEmpty()) {
            ArrayNode refsArr = contentNode.putArray("references");
            for (ArtifactReference ref : references) {
                ObjectNode r = refsArr.addObject();
                r.put("name", ref.name());
                r.put("groupId", ref.groupId());
                r.put("artifactId", ref.artifactId());
                if (ref.version() != null) r.put("version", ref.version());
            }
        }
        String path = "/apis/registry/v3/groups/" + urlEncode(groupId)
                + "/artifacts?ifExists=FIND_OR_CREATE_VERSION";
        JsonNode result = postJson(path, body);
        JsonNode versionMeta = result.has("version") ? result.get("version") : result;
        if (versionMeta != null && versionMeta.has("globalId")) {
            return versionMeta.get("globalId").asLong();
        }
        throw new ApicurioException("Apicurio response missing globalId: " + result);
    }

    private JsonNode getJson(String path) throws ApicurioException {
        HttpResponse<byte[]> resp = send(buildGet(path));
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("GET " + path + " → " + resp.statusCode());
        }
        try {
            return MAPPER.readTree(resp.body());
        } catch (Exception e) {
            throw new ApicurioException("Failed to parse JSON from " + path, e);
        }
    }

    private JsonNode getJsonOrNull(String path) throws ApicurioException {
        HttpResponse<byte[]> resp = send(buildGet(path));
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("GET " + path + " → " + resp.statusCode());
        }
        try {
            return MAPPER.readTree(resp.body());
        } catch (Exception e) {
            throw new ApicurioException("Failed to parse JSON from " + path, e);
        }
    }

    private byte[] getRaw(String path) throws ApicurioException {
        HttpResponse<byte[]> resp = send(buildGet(path));
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("GET " + path + " → " + resp.statusCode());
        }
        return resp.body();
    }

    private JsonNode postJson(String path, JsonNode body) throws ApicurioException {
        HttpRequest req;
        try {
            HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(30))
                    .POST(BodyPublishers.ofByteArray(MAPPER.writeValueAsBytes(body)));
            if (authHeader != null) b.header("Authorization", authHeader);
            req = b.build();
        } catch (Exception e) {
            throw new ApicurioException("Failed to build POST " + path, e);
        }
        HttpResponse<byte[]> resp = send(req);
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("POST " + path + " → " + resp.statusCode()
                    + " body=" + new String(resp.body()));
        }
        try {
            return MAPPER.readTree(resp.body());
        } catch (Exception e) {
            throw new ApicurioException("Failed to parse JSON from " + path, e);
        }
    }

    private HttpRequest buildGet(String path) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30))
                .GET();
        if (authHeader != null) b.header("Authorization", authHeader);
        return b.build();
    }

    private HttpResponse<byte[]> send(HttpRequest req) throws ApicurioException {
        try {
            return http.send(req, BodyHandlers.ofByteArray());
        } catch (Exception e) {
            throw new ApicurioException("HTTP request failed: " + req.uri(), e);
        }
    }

    private static String textOrNull(JsonNode n) {
        return n == null || n.isNull() ? null : n.asText();
    }

    private static String textOrDefault(JsonNode n, String fallback) {
        return n == null || n.isNull() ? fallback : n.asText();
    }

    private static String urlEncode(String s) {
        // Path-segment encoding — Apicurio accepts "default" + alnum + . _ - without escaping.
        return s.replace("/", "%2F");
    }

    @Override
    public void close() {
        // HttpClient (JDK) has no explicit close in JDK 21; rely on GC.
    }

    /** Marker thrown from any registry call. The SMT translates these via
     *  {@code behavior.on.error}. */
    public static class ApicurioException extends Exception {
        public ApicurioException(String message) { super(message); }
        public ApicurioException(String message, Throwable cause) { super(message, cause); }
    }
}
