package se.afshin.yavari.kafka.ui.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.oidc.AccessTokenCredential;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.rbac.RbacRulesService;
import se.afshin.yavari.kafka.ui.rbac.UserRbac;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Reads schema artifacts from the apicurio-rbac-proxy in front of Apicurio
 * Registry. Authentication is the same JWT the user logged in with — the
 * rbac-proxy enforces artifact-level access just like for direct CLI access.
 */
@ApplicationScoped
public class SchemaService {

    private static final Logger LOG = Logger.getLogger(SchemaService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Inject ClusterRegistry registry;
    @Inject AccessTokenCredential token;
    @Inject RbacRulesService rbacRules;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(TIMEOUT)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    public record SchemaSummary(String id, String type, String state, String createdOn) {}

    public record SchemaContent(String id, String type, String content, List<String> versions) {}

    public List<SchemaSummary> list(String clusterId, UserRbac rbac) {
        ClusterCoordinates c = coords(clusterId);
        // The apicurio-rbac-proxy doesn't whitelist /search/artifacts — it only
        // forwards per-artifact paths. Iterate the user's allow-list (or, for
        // wildcard access, every artifact name known across all KafkaRbac CRs)
        // and fetch each artifact's metadata directly.
        java.util.Set<String> candidates = rbac.allSchemasAllowed()
                ? rbacRules.allKnownSchemaArtifacts(c.namespace())
                : rbac.schemasAllowedToRead();

        List<SchemaSummary> out = new ArrayList<>();
        for (String id : candidates) {
            try {
                JsonNode meta = get(c.apicurioUrl()
                        + "/apis/registry/v2/groups/default/artifacts/" + id + "/meta");
                out.add(new SchemaSummary(
                        id,
                        meta.path("type").asText(""),
                        meta.path("state").asText(""),
                        meta.path("createdOn").asText("")));
            } catch (jakarta.ws.rs.WebApplicationException e) {
                // 404 = not registered yet, 403 = denied for this specific artifact.
                // Either way, skip silently from the list.
                LOG.debugf("Schema lookup skipped for %s: %s", id, e.getMessage());
            }
        }
        out.sort(Comparator.comparing(SchemaSummary::id));
        return out;
    }

    public SchemaContent get(String clusterId, String artifactId, UserRbac rbac) {
        if (!rbac.canReadSchema(artifactId)) {
            throw new WebApplicationException("Schema not visible", Response.Status.FORBIDDEN);
        }
        ClusterCoordinates c = coords(clusterId);
        JsonNode meta = get(c.apicurioUrl()
                + "/apis/registry/v2/groups/default/artifacts/" + artifactId + "/meta");
        String type = meta.path("type").asText("");

        // Latest content as raw text (could be JSON, Avro JSON, .proto, …)
        String content = getRaw(c.apicurioUrl()
                + "/apis/registry/v2/groups/default/artifacts/" + artifactId);
        content = prettify(content, type);

        JsonNode versions = get(c.apicurioUrl()
                + "/apis/registry/v2/groups/default/artifacts/" + artifactId + "/versions");
        List<String> versionIds = new ArrayList<>();
        for (JsonNode v : versions.path("versions")) {
            versionIds.add(v.path("version").asText());
        }

        return new SchemaContent(artifactId, type, content, versionIds);
    }

    /** JSON / JSON-Schema / Avro IDL get reflowed; .proto and unknown types pass through. */
    private static String prettify(String content, String type) {
        if (content == null || content.isBlank()) return content;
        String t = type == null ? "" : type.toUpperCase();
        if ("JSON".equals(t) || "AVRO".equals(t)) {
            try {
                return MAPPER.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(MAPPER.readTree(content));
            } catch (Exception ignored) {
                // Fall through to raw content
            }
        }
        return content;
    }

    private ClusterCoordinates coords(String clusterId) {
        return registry.byId(clusterId)
                .orElseThrow(() -> new WebApplicationException("Unknown cluster: " + clusterId,
                        Response.Status.NOT_FOUND));
    }

    private JsonNode get(String url) {
        try {
            String body = getRaw(url);
            return MAPPER.readTree(body);
        } catch (Exception e) {
            LOG.warnf(e, "Apicurio GET failed: %s", url);
            throw new WebApplicationException(e.getMessage(), Response.Status.BAD_GATEWAY);
        }
    }

    private String getRaw(String url) {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + token.getToken())
                .header("Accept", "application/json, */*")
                .GET()
                .build();
        try {
            HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 401 || resp.statusCode() == 403) {
                throw new WebApplicationException("Schema registry denied access (HTTP "
                        + resp.statusCode() + ")", resp.statusCode());
            }
            if (resp.statusCode() >= 400) {
                throw new WebApplicationException("Schema registry error (HTTP "
                        + resp.statusCode() + ")", Response.Status.BAD_GATEWAY);
            }
            return new String(resp.body(), StandardCharsets.UTF_8);
        } catch (WebApplicationException e) {
            throw e;
        } catch (Exception e) {
            LOG.warnf(e, "Apicurio HTTP call failed: %s", url);
            throw new WebApplicationException("Schema registry unreachable", Response.Status.BAD_GATEWAY);
        }
    }
}
