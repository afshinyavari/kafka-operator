package se.afshin.yavari.rbac;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
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
        String artifact      = extractArtifact(fullPath);
        PolicyEngine.Action action = resolveAction(method, fullPath);

        if (!policy.isAllowed(roles, artifact, action)) {
            return Response.status(403)
                .entity("Forbidden: roles " + roles + " cannot " + action + " artifact '" + artifact + "'")
                .build();
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
                if (HOP_BY_HOP.contains(name.toLowerCase())) return;
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
        // Apicurio globalId: /apis/registry/v2/ids/globalIds/1 -> * (artifact unknown)
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
}
