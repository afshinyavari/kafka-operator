package se.afshin.yavari.rbac;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.MDC;
import se.afshin.yavari.rbac.RegistryRequestClassifier.Classified;
import se.afshin.yavari.rbac.audit.AuditEmitter;
import se.afshin.yavari.rbac.audit.AuditEvent;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
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

    /** Carries the artifact id of a v2 create; v3 has it in the JSON body. */
    private static final String ARTIFACT_ID_HEADER = "X-Registry-ArtifactId";

    @Inject SecurityIdentity identity;
    @Inject PolicyEngine policy;
    @Inject ArtifactIdResolver resolver;
    @Inject AuditEmitter audit;

    @ConfigProperty(name = "proxy.apicurio.url")   String apicurioUrl;
    @ConfigProperty(name = "proxy.xml-schema.url") String xmlSchemaUrl;

    private final HttpClient http = HttpClient.newHttpClient();

    @GET
    @RunOnVirtualThread
    public Response get(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
        return proxy("GET", uriInfo, headers, null);
    }

    @POST
    @RunOnVirtualThread
    public Response post(@Context UriInfo uriInfo, @Context HttpHeaders headers, byte[] body) {
        return proxy("POST", uriInfo, headers, body);
    }

    @PUT
    @RunOnVirtualThread
    public Response put(@Context UriInfo uriInfo, @Context HttpHeaders headers, byte[] body) {
        return proxy("PUT", uriInfo, headers, body);
    }

    @DELETE
    @RunOnVirtualThread
    public Response delete(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
        return proxy("DELETE", uriInfo, headers, null);
    }

    private Response proxy(String method, UriInfo uriInfo, HttpHeaders requestHeaders, byte[] body) {
        // The path stays percent-encoded end to end: the classifier decodes it per segment and
        // the registry receives exactly what the client sent.
        String rawPath = uriInfo.getRequestUri().getRawPath();
        String rawQuery = uriInfo.getRequestUri().getRawQuery();
        Classified request = RegistryRequestClassifier.classify(method, rawPath, rawQuery,
                requestHeaders.getHeaderString(HttpHeaders.CONTENT_TYPE),
                requestHeaders.getHeaderString(ARTIFACT_ID_HEADER), body);
        PolicyEngine.Action action = request.action();

        // An id-only request is allowed when the caller may use any artifact holding that
        // content. An id the registry cannot resolve stays registry-wide ("*").
        List<String> artifacts = request.lookup() == null ? List.of() : resolver.resolve(request.lookup());
        if (artifacts.isEmpty()) artifacts = List.of(request.artifact());
        Optional<String> allowed = policy.firstAllowed(identity, artifacts, action);
        String artifact = allowed.orElse(artifacts.get(0));

        // Audit every request — allow, deny, or upstream error. Recorded in a try/finally so a
        // thrown exception from forward() is still captured as decision=error.
        long t0 = System.nanoTime();
        String decision = "error";
        try {
            if (allowed.isEmpty()) {
                decision = "deny";
                return RegistryErrors.forbidden(request.api(), action + " on artifact '" + artifact
                        + "' is not permitted for " + auditPrincipal());
            }
            String upstreamBase = upstreamBase(rawPath, apicurioUrl, xmlSchemaUrl);
            Response r = forward(request, method, upstreamBase, rawPath, rawQuery, requestHeaders, body);
            int status = r.getStatus();
            decision = status >= 500 ? "error" : status >= 400 ? "deny" : "allow";
            return r;
        } finally {
            long latencyMs = (System.nanoTime() - t0) / 1_000_000;
            audit.emit(new AuditEvent(Instant.now(), auditPrincipal(),
                    action.name(), artifact, decision, latencyMs, MDC.get("correlationId")));
        }
    }

    /** Every Apicurio API (core v2/v3, ccompat) lives under {@code /apis/}; anything else is
     *  the XML schema service. */
    static String upstreamBase(String rawPath, String apicurioUrl, String xmlSchemaUrl) {
        return rawPath.startsWith("/apis/") ? apicurioUrl : xmlSchemaUrl;
    }

    /** Audit principal for the current identity ({@code user:<name>} or {@code anonymous}). */
    String auditPrincipal() {
        return PolicyEngine.principalOf(identity, policy.principalMode());
    }

    private Response forward(Classified request, String method, String upstreamBase, String rawPath,
                             String rawQuery, HttpHeaders requestHeaders, byte[] body) {
        try {
            String upstreamUri = upstreamBase.replaceAll("/$", "") + rawPath
                + (rawQuery != null ? "?" + rawQuery : "");

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
            return RegistryErrors.badGateway(request.api(), "Bad gateway: " + e.getMessage());
        }
    }
}
