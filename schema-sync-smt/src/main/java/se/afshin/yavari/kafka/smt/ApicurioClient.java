package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal Apicurio Registry <strong>v2</strong> REST client. Avoids the
 * apicurio-registry-client SDK to keep the SMT JAR small and free of classloader
 * conflicts with Connect's own classpath. Apicurio 3.x still serves the v2 API as a
 * compatibility layer, so this works against both 2.x and 3.x.
 *
 * <p>Endpoints used:
 * <ul>
 *   <li>{@code GET  /apis/registry/v2/ids/globalIds/{id}} — raw schema content
 *   <li>{@code GET  /apis/registry/v2/ids/globalIds/{id}/references} — references
 *   <li>{@code GET  /apis/registry/v2/search/artifacts?globalId={id}} — artifact identity
 *       ({@code id} + {@code type}); v2 has no metadata-by-globalId endpoint
 *   <li>{@code GET  /apis/registry/v2/groups/{g}/artifacts/{a}[/versions/{v}]/meta} —
 *       reference coordinates → globalId
 *   <li>{@code POST /apis/registry/v2/groups/{g}/artifacts?ifExists=RETURN_OR_UPDATE} —
 *       create/return; identity + type travel in {@code X-Registry-*} headers
 * </ul>
 *
 * <p>Authentication is supplied by an {@link AuthProvider}: none, a static header, or an
 * OAuth2 client-credentials provider that refreshes tokens (see {@link OAuthTokenProvider}).
 * When a request comes back 401/403, the client asks the provider to refresh and retries
 * once — this keeps a long-lived MirrorMaker2 worker authenticated as tokens expire.
 */
public class ApicurioClient extends RestRegistryClient {

    private static final String V2 = "/apis/registry/v2";

    /** Supplies the {@code Authorization} header for each request and refreshes credentials
     *  after an authentication failure. */
    public interface AuthProvider {
        /** Header value (e.g. {@code "Bearer …"}), or {@code null} for no auth. */
        String header() throws RegistryException;

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
                public String header() throws RegistryException { return "Bearer " + provider.token(); }
                public boolean refresh() { provider.invalidate(); return true; }
            };
        }
    }

    /** Backwards-compatible constructor: a static (or absent) {@code Authorization} header. */
    public ApicurioClient(String baseUrl, String authHeader) {
        this(baseUrl, AuthProvider.staticHeader(authHeader), null);
    }

    public ApicurioClient(String baseUrl, AuthProvider auth) {
        this(baseUrl, auth, null);
    }

    public ApicurioClient(String baseUrl, AuthProvider auth, SSLContext ssl) {
        super(baseUrl, auth, ssl);
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

    @Override
    public RegistrySchema fetchById(long globalId) throws RegistryException {
        byte[] content = getRawOrNull(V2 + "/ids/globalIds/" + globalId);
        if (content == null) {
            throw RegistryException.notFound("No schema for globalId " + globalId + " at " + baseUrl);
        }
        // Identity — v2 has no metadata-by-globalId endpoint, so search for the artifact
        // owning the version with this globalId. A globalId belongs to exactly one
        // artifact version in Apicurio, so the hit is unambiguous. The result carries
        // `id` and `type`; `groupId` is omitted for the default group.
        JsonNode search = getJson(V2 + "/search/artifacts?globalId=" + globalId);
        JsonNode artifacts = search.get("artifacts");
        if (artifacts == null || !artifacts.isArray() || artifacts.isEmpty()) {
            throw RegistryException.notFound("No artifact owns globalId " + globalId + " at " + baseUrl);
        }
        JsonNode a = artifacts.get(0);
        String groupId = textOrDefault(a.get("groupId"), "default");
        String artifactId = a.get("id").asText();
        String artifactType = textOrDefault(a.get("type"), "JSON");
        List<SchemaRef> refs = new ArrayList<>();
        JsonNode refNode = getJsonOrNull(V2 + "/ids/globalIds/" + globalId + "/references");
        if (refNode != null && refNode.isArray()) {
            for (JsonNode r : refNode) {
                refs.add(new SchemaRef(
                        textOrNull(r.get("name")),
                        textOrDefault(r.get("groupId"), "default"),
                        textOrNull(r.get("artifactId")),
                        textOrNull(r.get("version"))));
            }
        }
        return new RegistrySchema(groupId, artifactId, artifactType, content, refs);
    }

    /** Creates (or returns the existing identical) artifact. Returns the globalId and
     *  version assigned by this registry. */
    @Override
    public Registered upsert(RegistrySchema schema) throws RegistryException {
        // v2: identity + type go in X-Registry-* headers. The body is the raw schema
        // content — unless there are references, which require the extended envelope.
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Registry-ArtifactId", schema.artifactId());
        headers.put("X-Registry-ArtifactType", schema.type());
        byte[] body;
        if (schema.references() != null && !schema.references().isEmpty()) {
            ObjectNode env = MAPPER.createObjectNode();
            env.put("content", new String(schema.content(), StandardCharsets.UTF_8));
            ArrayNode refsArr = env.putArray("references");
            for (SchemaRef ref : schema.references()) {
                ObjectNode r = refsArr.addObject();
                r.put("groupId", ref.groupId());
                r.put("artifactId", ref.artifactId());
                if (ref.version() != null) r.put("version", ref.version());
                if (ref.name() != null) r.put("name", ref.name());
            }
            try {
                body = MAPPER.writeValueAsBytes(env);
            } catch (Exception e) {
                throw new RegistryException("Failed to serialise artifact envelope", e);
            }
            headers.put("Content-Type", "application/create.extended+json");
        } else {
            body = schema.content();
            headers.put("Content-Type", "application/json");
        }
        String path = V2 + "/groups/" + pathSegment(schema.groupId())
                + "/artifacts?ifExists=RETURN_OR_UPDATE";
        JsonNode result = postJson(path, body, headers);
        if (result == null || !result.has("globalId")) {
            throw new RegistryException("Apicurio response missing globalId: " + result);
        }
        return new Registered(result.get("globalId").asLong(), textOrNull(result.get("version")));
    }

    @Override
    public Long lookupId(SchemaRef ref) throws RegistryException {
        String base = V2 + "/groups/" + pathSegment(ref.groupId()) + "/artifacts/"
                + pathSegment(ref.artifactId());
        String path = ref.version() == null ? base + "/meta"
                : base + "/versions/" + pathSegment(ref.version()) + "/meta";
        JsonNode meta = getJsonOrNull(path);
        return meta == null || !meta.has("globalId") ? null : meta.get("globalId").asLong();
    }
}
