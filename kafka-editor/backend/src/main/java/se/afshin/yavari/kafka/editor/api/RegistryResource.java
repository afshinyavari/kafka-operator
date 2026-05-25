package se.afshin.yavari.kafka.editor.api;

import io.quarkus.security.Authenticated;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Proxies schema-registry calls (Apicurio v2 API) so the browser can reach an
 * arbitrary, user-configured registry URL without CORS — search and read, plus
 * artifact create / update / delete.
 */
@Authenticated
@Path("/api/registry")
public class RegistryResource {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /** Search artifacts in the registry. */
    @GET
    @Path("/artifacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response artifacts(
            @QueryParam("registry") String registry,
            @QueryParam("name") String name) {
        if (registry == null || registry.isBlank()) {
            return error(400, "Missing registry URL");
        }
        StringBuilder url = new StringBuilder(base(registry))
                .append("/apis/registry/v2/search/artifacts?limit=100");
        if (name != null && !name.isBlank()) {
            url.append("&name=")
                    .append(URLEncoder.encode(name.trim(), StandardCharsets.UTF_8));
        }
        return proxy(HttpRequest.newBuilder(URI.create(url.toString())).GET());
    }

    /** Fetch the latest content of an artifact. */
    @GET
    @Path("/content")
    @Produces(MediaType.APPLICATION_JSON)
    public Response content(
            @QueryParam("registry") String registry,
            @QueryParam("groupId") String groupId,
            @QueryParam("artifactId") String artifactId) {
        if (registry == null || registry.isBlank()
                || artifactId == null || artifactId.isBlank()) {
            return error(400, "Missing registry URL or artifact id");
        }
        return proxy(HttpRequest.newBuilder(
                URI.create(artifactUrl(registry, groupId, artifactId))).GET());
    }

    /** Artifact metadata (includes the global id used by the Apicurio envelope). */
    @GET
    @Path("/meta")
    @Produces(MediaType.APPLICATION_JSON)
    public Response meta(
            @QueryParam("registry") String registry,
            @QueryParam("groupId") String groupId,
            @QueryParam("artifactId") String artifactId) {
        if (registry == null || registry.isBlank()
                || artifactId == null || artifactId.isBlank()) {
            return error(400, "Missing registry URL or artifact id");
        }
        return proxy(HttpRequest.newBuilder(URI.create(
                artifactUrl(registry, groupId, artifactId) + "/meta")).GET());
    }

    /** List an artifact's versions. */
    @GET
    @Path("/versions")
    @Produces(MediaType.APPLICATION_JSON)
    public Response versions(
            @QueryParam("registry") String registry,
            @QueryParam("groupId") String groupId,
            @QueryParam("artifactId") String artifactId) {
        if (registry == null || registry.isBlank()
                || artifactId == null || artifactId.isBlank()) {
            return error(400, "Missing registry URL or artifact id");
        }
        return proxy(HttpRequest.newBuilder(URI.create(
                artifactUrl(registry, groupId, artifactId) + "/versions")).GET());
    }

    /** Create a new artifact. */
    @POST
    @Path("/artifacts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(SchemaWriteRequest request) {
        if (request == null || request.registry() == null
                || request.registry().isBlank()
                || request.artifactId() == null || request.artifactId().isBlank()
                || request.content() == null) {
            return error(400, "Missing registry URL, artifact id or content");
        }
        String url = base(request.registry()) + "/apis/registry/v2/groups/"
                + URLEncoder.encode(request.groupOrDefault(), StandardCharsets.UTF_8)
                + "/artifacts";
        return proxy(HttpRequest.newBuilder(URI.create(url))
                .header("X-Registry-ArtifactId", request.artifactId().trim())
                .header("X-Registry-ArtifactType",
                        request.type() == null ? "AVRO"
                                : request.type().trim().toUpperCase())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(request.content())));
    }

    /** Publish a new version of an existing artifact. */
    @PUT
    @Path("/artifacts")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response update(SchemaWriteRequest request) {
        if (request == null || request.registry() == null
                || request.registry().isBlank()
                || request.artifactId() == null || request.artifactId().isBlank()
                || request.content() == null) {
            return error(400, "Missing registry URL, artifact id or content");
        }
        return proxy(HttpRequest.newBuilder(URI.create(artifactUrl(
                        request.registry(), request.groupId(),
                        request.artifactId())))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(request.content())));
    }

    /** Delete an artifact. */
    @DELETE
    @Path("/artifacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete(
            @QueryParam("registry") String registry,
            @QueryParam("groupId") String groupId,
            @QueryParam("artifactId") String artifactId) {
        if (registry == null || registry.isBlank()
                || artifactId == null || artifactId.isBlank()) {
            return error(400, "Missing registry URL or artifact id");
        }
        return proxy(HttpRequest.newBuilder(
                URI.create(artifactUrl(registry, groupId, artifactId))).DELETE());
    }

    private Response proxy(HttpRequest.Builder builder) {
        try {
            HttpResponse<String> response = HTTP.send(
                    builder.timeout(Duration.ofSeconds(8)).build(),
                    HttpResponse.BodyHandlers.ofString());
            String body = response.body() == null || response.body().isBlank()
                    ? "{}" : response.body();
            return Response.status(response.statusCode())
                    .type(MediaType.APPLICATION_JSON)
                    .entity(body)
                    .build();
        } catch (Exception e) {
            return error(502, "Could not reach the schema registry.");
        }
    }

    private static String artifactUrl(String registry, String groupId,
            String artifactId) {
        String group = groupId == null || groupId.isBlank() ? "default" : groupId;
        return base(registry) + "/apis/registry/v2/groups/"
                + URLEncoder.encode(group, StandardCharsets.UTF_8)
                + "/artifacts/"
                + URLEncoder.encode(artifactId, StandardCharsets.UTF_8);
    }

    private static String base(String registry) {
        String trimmed = registry.trim();
        return trimmed.endsWith("/")
                ? trimmed.substring(0, trimmed.length() - 1)
                : trimmed;
    }

    private static Response error(int status, String message) {
        return Response.status(status)
                .type(MediaType.APPLICATION_JSON)
                .entity("{\"error\":\"" + message + "\"}")
                .build();
    }
}
