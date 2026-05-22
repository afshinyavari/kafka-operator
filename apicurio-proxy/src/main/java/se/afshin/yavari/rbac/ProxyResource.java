package se.afshin.yavari.rbac;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("{path: .*}")
@Authenticated
@Consumes(MediaType.WILDCARD)
@Produces(MediaType.WILDCARD)
public class ProxyResource {

    private static final Set<String> HOP_BY_HOP = Set.of(
        "connection", "content-length", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade", "host"
    );

    @Inject SecurityIdentity identity;
    @Inject PolicyEngine policy;

    @ConfigProperty(name = "proxy.apicurio.url")   String apicurioUrl;
    @ConfigProperty(name = "proxy.xml-schema.url") String xmlSchemaUrl;

    private final HttpClient http = HttpClient.newHttpClient();

    @GET
    @RunOnVirtualThread
    public Response get(@PathParam("path") String path, @Context UriInfo uriInfo,
                        @Context HttpHeaders headers) {
        return proxy("GET", path, uriInfo, headers, null);
    }

    @POST
    @RunOnVirtualThread
    public Response post(@PathParam("path") String path, @Context UriInfo uriInfo,
                         @Context HttpHeaders headers, byte[] body) {
        return proxy("POST", path, uriInfo, headers, body);
    }

    @PUT
    @RunOnVirtualThread
    public Response put(@PathParam("path") String path, @Context UriInfo uriInfo,
                        @Context HttpHeaders headers, byte[] body) {
        return proxy("PUT", path, uriInfo, headers, body);
    }

    @DELETE
    @RunOnVirtualThread
    public Response delete(@PathParam("path") String path, @Context UriInfo uriInfo,
                           @Context HttpHeaders headers) {
        return proxy("DELETE", path, uriInfo, headers, null);
    }

    private Response proxy(String method, String pathParam, UriInfo uriInfo,
                           HttpHeaders requestHeaders, byte[] body) {
        String fullPath = "/" + pathParam;
        Set<String> roles    = identity.getRoles();
        String artifact      = resolveArtifact(fullPath, uriInfo.getRequestUri().getRawQuery());
        PolicyEngine.Action action = resolveAction(method, fullPath);

        if (!policy.isAllowed(roles, artifact, action)) {
            return Response.status(403).entity("Forbidden").build();
        }

        String upstreamBase = fullPath.startsWith("/apis/registry/") ? apicurioUrl : xmlSchemaUrl;
        return forward(method, upstreamBase, fullPath, uriInfo, requestHeaders, body);
    }

    private Response forward(String method, String upstreamBase, String path,
                             UriInfo uriInfo, HttpHeaders requestHeaders, byte[] body) {
        try {
            String query = uriInfo.getRequestUri().getRawQuery();
            String upstreamUri = upstreamBase.replaceAll("/$", "") + path
                + (query != null ? "?" + query : "");

            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(URI.create(upstreamUri));

            requestHeaders.getRequestHeaders().forEach((name, values) -> {
                String lower = name.toLowerCase();
                if ("authorization".equals(lower) || HOP_BY_HOP.contains(lower)) return;
                values.forEach(v -> reqBuilder.header(name, v));
            });

            HttpRequest.BodyPublisher publisher = (body != null && body.length > 0)
                ? HttpRequest.BodyPublishers.ofByteArray(body)
                : HttpRequest.BodyPublishers.noBody();
            reqBuilder.method(method, publisher);

            HttpResponse<byte[]> upstream = http.send(reqBuilder.build(),
                                                      HttpResponse.BodyHandlers.ofByteArray());

            Response.ResponseBuilder rb = Response.status(upstream.statusCode());
            upstream.headers().map().forEach((name, values) -> {
                // Skip HTTP/2 pseudo-headers (:status, :path, etc.) — invalid in HTTP/1 responses
                if (name.startsWith(":") || HOP_BY_HOP.contains(name.toLowerCase())) return;
                values.forEach(v -> rb.header(name, v));
            });

            byte[] respBody = upstream.body();
            if (respBody != null && respBody.length > 0) {
                rb.entity(respBody);
            }
            return rb.build();

        } catch (Exception e) {
            return Response.status(502).entity("Bad gateway: " + e.getMessage()).build();
        }
    }

    static PolicyEngine.Action resolveAction(String method, String path) {
        return switch (method.toUpperCase()) {
            case "GET", "HEAD" -> PolicyEngine.Action.READ;
            case "DELETE"      -> PolicyEngine.Action.DELETE;
            default            -> PolicyEngine.Action.WRITE;
        };
    }

    static String extractArtifact(String path) {
        // Apicurio: /apis/registry/v2/groups/default/artifacts/orders -> orders
        // Apicurio by-id: /apis/registry/v2/ids/globalIds/1 -> * here; resolveArtifact()
        //   resolves the real artifact from the registry before this fallback applies.
        // XML: /schemas/orders -> orders
        // XML list: /schemas -> *
        String[] parts = path.split("/");
        if (path.startsWith("/apis/registry/")) {
            if (path.contains("/ids/")) return "*";
            for (int i = 0; i < parts.length - 1; i++) {
                if ("artifacts".equals(parts[i])) return parts[i + 1];
            }
            return "*";
        }
        if (path.startsWith("/schemas/") && parts.length > 2) {
            return parts[2];
        }
        return "*";
    }

    /**
     * Resolves the policy artifact for a request. For most paths this is just
     * {@link #extractArtifact}. Two id-based forms carry no artifact name and are
     * resolved from the registry's search API so RBAC applies against the real name:
     * a by-id lookup ({@code /ids/{globalIds,contentIds}/{id}}) and a search-by-id
     * query ({@code search/artifacts?globalId=} / {@code ?contentId=}) — together
     * these are how a generic consumer/UI fetches a schema's content and its type.
     * Falls back to {@link #extractArtifact} ({@code "*"}) when the id can't resolve.
     */
    String resolveArtifact(String fullPath, String rawQuery) {
        IdLookup lookup = parseIdLookup(fullPath);
        if (lookup == null) lookup = parseSearchByIdQuery(fullPath, rawQuery);
        if (lookup != null) {
            String resolved = lookupArtifactId(lookup);
            if (resolved != null) return resolved;
        }
        return extractArtifact(fullPath);
    }

    /** A by-id schema lookup and the registry search parameter that resolves it. */
    record IdLookup(String queryParam, String id) {}

    /**
     * Parses an Apicurio by-id lookup path. {@code /apis/registry/v2/ids/globalIds/{id}}
     * and {@code .../ids/contentIds/{id}} (incl. trailing sub-paths like
     * {@code /references}) name a schema without an artifact. Returns {@code null} for
     * any other path, including content-hash lookups (no single search param for them).
     */
    static IdLookup parseIdLookup(String path) {
        if (!path.startsWith("/apis/registry/")) return null;
        String[] parts = path.split("/");
        for (int i = 0; i + 2 < parts.length; i++) {
            if (!"ids".equals(parts[i])) continue;
            String id = parts[i + 2];
            if (id.isBlank()) return null;
            return switch (parts[i + 1]) {
                case "globalIds"  -> new IdLookup("globalId", id);
                case "contentIds" -> new IdLookup("contentId", id);
                default           -> null;
            };
        }
        return null;
    }

    /**
     * Parses a {@code search/artifacts?globalId={id}} (or {@code ?contentId={id}})
     * query — how a generic Apicurio consumer / the Kafka UI resolves a schema's
     * type by id. Authorizing it against the resolved artifact (not {@code "*"})
     * lets a scoped role look up the type of a schema it is already granted.
     * Returns {@code null} for a general search (e.g. {@code ?name=}), which stays
     * a registry-wide operation requiring a wildcard grant.
     */
    static IdLookup parseSearchByIdQuery(String path, String rawQuery) {
        if (rawQuery == null || !path.startsWith("/apis/registry/")
                || !path.endsWith("/search/artifacts")) {
            return null;
        }
        for (String param : rawQuery.split("&")) {
            int eq = param.indexOf('=');
            if (eq <= 0 || eq == param.length() - 1) continue;
            String key = param.substring(0, eq);
            String val = param.substring(eq + 1);
            if ("globalId".equals(key))  return new IdLookup("globalId", val);
            if ("contentId".equals(key)) return new IdLookup("contentId", val);
        }
        return null;
    }

    /** Resolves the owning artifact id for a by-id lookup via the registry search API. */
    private String lookupArtifactId(IdLookup lookup) {
        try {
            String uri = apicurioUrl.replaceAll("/$", "")
                + "/apis/registry/v2/search/artifacts?" + lookup.queryParam() + "=" + lookup.id();
            HttpResponse<String> resp = http.send(
                HttpRequest.newBuilder(URI.create(uri)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 ? firstArtifactId(resp.body()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Extracts the first artifact id from an Apicurio v2 {@code search/artifacts} response. */
    static String firstArtifactId(String json) {
        Object root = new Yaml().load(json);   // JSON is valid YAML — reuse snakeyaml
        if (root instanceof Map<?, ?> map && map.get("artifacts") instanceof List<?> artifacts
                && !artifacts.isEmpty() && artifacts.get(0) instanceof Map<?, ?> first) {
            Object id = first.get("id");
            return id != null ? id.toString() : null;
        }
        return null;
    }
}
