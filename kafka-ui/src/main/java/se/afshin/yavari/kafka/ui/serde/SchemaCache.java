package se.afshin.yavari.kafka.ui.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process cache for Apicurio schema metadata + content.
 *
 * <p>Keys:
 * <ul>
 *   <li>{@link #byGlobalId} — long-TTL, since globalId is immutable in Apicurio</li>
 *   <li>{@link #byArtifact} — short TTL (30 s) because the version pointer moves</li>
 * </ul>
 * Both methods cache <em>negative</em> results too: a 404 sticks for 30 s so the
 * deserializer doesn't hammer Apicurio for unknown ids encountered repeatedly.
 *
 * <p>Each lookup uses the calling request's bearer token. The cache does not key
 * on the token — Apicurio data does not vary by user (the rbac-proxy is purely
 * a 403/200 gate, not a content filter), so it is safe to share entries across
 * users.
 */
@ApplicationScoped
public class SchemaCache {

    private static final Logger LOG = Logger.getLogger(SchemaCache.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Duration POSITIVE_TTL = Duration.ofMinutes(30);
    private static final Duration NEGATIVE_TTL = Duration.ofSeconds(30);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(3);

    public record SchemaMeta(String type, String content, long globalId) {}

    private record Entry<T>(T value, Instant expires) {
        boolean fresh() { return Instant.now().isBefore(expires); }
        boolean missing() { return value == null; }
    }

    private final ConcurrentHashMap<Long, Entry<SchemaMeta>> byGlobalId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Entry<SchemaMeta>> byArtifact = new ConcurrentHashMap<>();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build();

    public Optional<SchemaMeta> byGlobalId(long gid, String apicurioUrl, String bearer) {
        Entry<SchemaMeta> e = byGlobalId.get(gid);
        if (e != null && e.fresh()) return Optional.ofNullable(e.value);

        try {
            JsonNode meta = getJson(apicurioUrl + "/apis/registry/v2/ids/globalIds/" + gid + "/meta", bearer);
            String content = getRaw(apicurioUrl + "/apis/registry/v2/ids/globalIds/" + gid, bearer);
            SchemaMeta sm = new SchemaMeta(meta.path("type").asText(""), content, gid);
            byGlobalId.put(gid, new Entry<>(sm, Instant.now().plus(POSITIVE_TTL)));
            return Optional.of(sm);
        } catch (NotFound nf) {
            byGlobalId.put(gid, new Entry<>(null, Instant.now().plus(NEGATIVE_TTL)));
            return Optional.empty();
        } catch (Exception ex) {
            LOG.debugf(ex, "byGlobalId(%d) lookup failed", gid);
            return Optional.empty();
        }
    }

    public Optional<SchemaMeta> byArtifact(String artifactId, String apicurioUrl, String bearer) {
        Entry<SchemaMeta> e = byArtifact.get(artifactId);
        if (e != null && e.fresh()) return Optional.ofNullable(e.value);

        try {
            JsonNode meta = getJson(apicurioUrl
                    + "/apis/registry/v2/groups/default/artifacts/" + artifactId + "/meta", bearer);
            String content = getRaw(apicurioUrl
                    + "/apis/registry/v2/groups/default/artifacts/" + artifactId, bearer);
            SchemaMeta sm = new SchemaMeta(
                    meta.path("type").asText(""),
                    content,
                    meta.path("globalId").asLong(-1));
            byArtifact.put(artifactId, new Entry<>(sm, Instant.now().plus(NEGATIVE_TTL)));
            return Optional.of(sm);
        } catch (NotFound nf) {
            byArtifact.put(artifactId, new Entry<>(null, Instant.now().plus(NEGATIVE_TTL)));
            return Optional.empty();
        } catch (Exception ex) {
            LOG.debugf(ex, "byArtifact(%s) lookup failed", artifactId);
            return Optional.empty();
        }
    }

    public void invalidateAll() {
        byGlobalId.clear();
        byArtifact.clear();
    }

    private JsonNode getJson(String url, String bearer) throws Exception {
        return MAPPER.readTree(getRaw(url, bearer));
    }

    private String getRaw(String url, String bearer) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(HTTP_TIMEOUT)
                .header("Authorization", "Bearer " + bearer)
                .header("Accept", "application/json, */*")
                .GET()
                .build();
        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() == 404) throw new NotFound();
        if (resp.statusCode() >= 400) {
            throw new RuntimeException("Apicurio " + url + " → HTTP " + resp.statusCode());
        }
        return new String(resp.body(), StandardCharsets.UTF_8);
    }

    private static final class NotFound extends RuntimeException {}
}
