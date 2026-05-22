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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal Apicurio Registry <strong>v2</strong> REST client used by
 * {@link ApicurioSchemaTransferSmt}. Avoids the apicurio-registry-client SDK to keep the
 * SMT JAR small and free of classloader conflicts with Connect's own classpath.
 *
 * <p>The operator deploys the {@code apicurio-registry-kafkasql} image, which serves the
 * v2 API ({@code /apis/registry/v2}); the v3 API is not present. Endpoints used:
 * <ul>
 *   <li>{@code GET  /apis/registry/v2/ids/globalIds/{id}} — raw schema content
 *   <li>{@code GET  /apis/registry/v2/ids/globalIds/{id}/references} — references
 *   <li>{@code GET  /apis/registry/v2/search/artifacts?globalId={id}} — artifact identity
 *       ({@code id} + {@code type}); v2 has no metadata-by-globalId endpoint
 *   <li>{@code POST /apis/registry/v2/groups/{g}/artifacts?ifExists=RETURN_OR_UPDATE} —
 *       create/return; identity + type travel in {@code X-Registry-*} headers
 * </ul>
 *
 * <p>Authentication is supplied by an {@link AuthProvider}: none, a static header, or an
 * OAuth2 client-credentials provider that refreshes tokens (see {@link OAuthTokenProvider}).
 * When a request comes back 401/403, the client asks the provider to refresh and retries
 * once — this keeps a long-lived MirrorMaker2 worker authenticated as tokens expire.
 */
public class ApicurioClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String V2 = "/apis/registry/v2";

    private final String baseUrl;
    private final HttpClient http;
    private final AuthProvider auth;

    /** Supplies the {@code Authorization} header for each request and refreshes credentials
     *  after an authentication failure. */
    public interface AuthProvider {
        /** Header value (e.g. {@code "Bearer …"}), or {@code null} for no auth. */
        String header() throws ApicurioException;

        /** Forces a credential refresh after a 401/403. Returns {@code true} if a retry
         *  may now succeed (i.e. the credentials are refreshable). */
        boolean refresh();

        /** No authentication. */
        static AuthProvider none() {
            return new AuthProvider() {
                public String header() { return null; }
                public boolean refresh() { return false; }
            };
        }

        /** A fixed, pre-formed header value (e.g. {@code "Basic …"} or {@code "Bearer …"}). */
        static AuthProvider staticHeader(String value) {
            if (value == null) return none();
            return new AuthProvider() {
                public String header() { return value; }
                public boolean refresh() { return false; }
            };
        }

        /** OAuth2 client-credentials — fetches and refreshes bearer tokens. */
        static AuthProvider oauth(OAuthTokenProvider provider) {
            return new AuthProvider() {
                public String header() throws ApicurioException { return "Bearer " + provider.token(); }
                public boolean refresh() { provider.invalidate(); return true; }
            };
        }
    }

    /** Backwards-compatible constructor: a static (or absent) {@code Authorization} header. */
    public ApicurioClient(String baseUrl, String authHeader) {
        this(baseUrl, AuthProvider.staticHeader(authHeader));
    }

    public ApicurioClient(String baseUrl, AuthProvider auth) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.auth = auth != null ? auth : AuthProvider.none();
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

    /** Fetches the artifact content + identity for a given globalId from this registry. */
    public ArtifactByGlobalId fetchByGlobalId(long globalId) throws ApicurioException {
        // Content.
        byte[] content = getRaw(V2 + "/ids/globalIds/" + globalId);
        // Identity — v2 has no metadata-by-globalId endpoint, so search for the artifact
        // owning the version with this globalId. The result carries `id` and `type`;
        // `groupId` is omitted for the default group.
        JsonNode search = getJson(V2 + "/search/artifacts?globalId=" + globalId);
        JsonNode artifacts = search.get("artifacts");
        if (artifacts == null || !artifacts.isArray() || artifacts.isEmpty()) {
            throw new ApicurioException("No artifact found for globalId " + globalId);
        }
        JsonNode a = artifacts.get(0);
        String groupId = textOrDefault(a.get("groupId"), "default");
        String artifactId = a.get("id").asText();
        String artifactType = textOrDefault(a.get("type"), "JSON");
        // References.
        List<ArtifactReference> refs = new ArrayList<>();
        JsonNode refNode = getJsonOrNull(V2 + "/ids/globalIds/" + globalId + "/references");
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

    /** Creates (or returns the existing identical) artifact in this registry. Returns the
     *  globalId assigned by this registry. */
    public long upsertArtifact(String groupId, String artifactId, String artifactType,
                                byte[] content, List<ArtifactReference> references)
            throws ApicurioException {
        // v2: identity + type go in X-Registry-* headers. The body is the raw schema
        // content — unless there are references, which require the extended envelope.
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Registry-ArtifactId", artifactId);
        headers.put("X-Registry-ArtifactType", artifactType);
        byte[] body;
        if (references != null && !references.isEmpty()) {
            ObjectNode env = MAPPER.createObjectNode();
            env.put("content", new String(content, StandardCharsets.UTF_8));
            ArrayNode refsArr = env.putArray("references");
            for (ArtifactReference ref : references) {
                ObjectNode r = refsArr.addObject();
                r.put("groupId", ref.groupId());
                r.put("artifactId", ref.artifactId());
                if (ref.version() != null) r.put("version", ref.version());
                if (ref.name() != null) r.put("name", ref.name());
            }
            try {
                body = MAPPER.writeValueAsBytes(env);
            } catch (Exception e) {
                throw new ApicurioException("Failed to serialise artifact envelope", e);
            }
            headers.put("Content-Type", "application/create.extended+json");
        } else {
            body = content;
            headers.put("Content-Type", "application/json");
        }
        String path = V2 + "/groups/" + urlEncode(groupId)
                + "/artifacts?ifExists=RETURN_OR_UPDATE";
        HttpResponse<byte[]> resp = exchange("POST", path, body, headers);
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("POST " + path + " → " + resp.statusCode()
                    + " body=" + new String(resp.body(), StandardCharsets.UTF_8));
        }
        JsonNode result;
        try {
            result = MAPPER.readTree(resp.body());
        } catch (Exception e) {
            throw new ApicurioException("Failed to parse JSON from " + path, e);
        }
        if (result != null && result.has("globalId")) {
            return result.get("globalId").asLong();
        }
        throw new ApicurioException("Apicurio response missing globalId: " + result);
    }

    private JsonNode getJson(String path) throws ApicurioException {
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
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
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
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
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("GET " + path + " → " + resp.statusCode());
        }
        return resp.body();
    }

    /** Sends a request with the current auth header; on a 401/403 refreshes the credentials
     *  (if refreshable) and retries exactly once. */
    private HttpResponse<byte[]> exchange(String method, String path, byte[] body,
                                          Map<String, String> headers) throws ApicurioException {
        HttpResponse<byte[]> resp = send(method, path, body, headers);
        if ((resp.statusCode() == 401 || resp.statusCode() == 403) && auth.refresh()) {
            LOG.debug("Auth rejected ({}) on {} {} — refreshed credentials, retrying",
                    resp.statusCode(), method, path);
            resp = send(method, path, body, headers);
        }
        return resp;
    }

    private HttpResponse<byte[]> send(String method, String path, byte[] body,
                                      Map<String, String> headers) throws ApicurioException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30));
        if (headers != null) {
            headers.forEach(b::header);
        }
        if ("POST".equals(method)) {
            b.POST(BodyPublishers.ofByteArray(body));
        } else {
            b.GET();
        }
        String header = auth.header();
        if (header != null) b.header("Authorization", header);
        try {
            return http.send(b.build(), BodyHandlers.ofByteArray());
        } catch (Exception e) {
            throw new ApicurioException("HTTP request failed: " + method + " " + baseUrl + path, e);
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
