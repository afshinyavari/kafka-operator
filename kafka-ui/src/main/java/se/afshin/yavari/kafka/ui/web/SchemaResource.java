package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.afshin.yavari.kafka.ui.audit.AuditLog;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.service.SchemaService;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaContent;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaSummary;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/clusters/{id}/schemas")
@Authenticated
public class SchemaResource {

    private static final Set<String> ALLOWED_TYPES = Set.of("AVRO", "JSON", "PROTOBUF", "JSONSCHEMA");

    @Inject ClusterRegistry registry;
    @Inject SchemaService schemas;
    @Inject UserContext user;
    @Inject AuditLog audit;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<SchemaSummary> schemas,
                                                   String username,
                                                   String errorMessage,
                                                   String successMessage);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     SchemaContent schema,
                                                     String username,
                                                     String errorMessage,
                                                     String successMessage);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @QueryParam("error") String error,
                                 @QueryParam("success") String success) {
        ClusterCoordinates c = cluster(id);
        return Templates.list(c, schemas.list(id, user.rbac(c.namespace())), user.username(), error, success);
    }

    @GET
    @Path("/{artifactId}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                                   @QueryParam("error") String error,
                                   @QueryParam("success") String success) {
        ClusterCoordinates c = cluster(id);
        return Templates.detail(c, schemas.get(id, artifactId, user.rbac(c.namespace())),
                user.username(), error, success);
    }

    @POST
    public Response create(@PathParam("id") String id,
                           @FormParam("artifactId") String artifactId,
                           @FormParam("type") String type,
                           @FormParam("content") String content) {
        cluster(id);
        String upperType = type == null ? "" : type.toUpperCase();
        if (artifactId == null || artifactId.isBlank()) {
            return errorList(id, "Artifact ID is required.");
        }
        if (!ALLOWED_TYPES.contains(upperType)) {
            return errorList(id, "Type must be one of: " + ALLOWED_TYPES + ".");
        }
        try {
            schemas.createArtifact(id, artifactId, upperType, content);
            audit.success(user.username(), "schema.create", id + "/" + artifactId, Map.of("type", upperType));
            return successList(id, "Schema '" + artifactId + "' created.");
        } catch (IllegalArgumentException e) {
            return errorList(id, e.getMessage());
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.create", id + "/" + artifactId, reason);
            return errorList(id, "Create failed: " + reason);
        }
    }

    @POST
    @Path("/{artifactId}/versions")
    public Response newVersion(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                               @FormParam("type") String type,
                               @FormParam("content") String content) {
        cluster(id);
        String upperType = type == null ? "" : type.toUpperCase();
        if (!ALLOWED_TYPES.contains(upperType)) {
            return errorDetail(id, artifactId, "Type must be one of: " + ALLOWED_TYPES + ".");
        }
        try {
            schemas.updateArtifact(id, artifactId, upperType, content);
            audit.success(user.username(), "schema.newVersion", id + "/" + artifactId, Map.of("type", upperType));
            return successDetail(id, artifactId, "New version of '" + artifactId + "' published.");
        } catch (IllegalArgumentException e) {
            return errorDetail(id, artifactId, e.getMessage());
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.newVersion", id + "/" + artifactId, reason);
            return errorDetail(id, artifactId, "Update failed: " + reason);
        }
    }

    @POST
    @Path("/{artifactId}/delete")
    public Response delete(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                           @FormParam("confirm") String confirm) {
        cluster(id);
        if (!artifactId.equals(confirm)) {
            return errorDetail(id, artifactId, "Delete cancelled: confirmation did not match.");
        }
        try {
            schemas.deleteArtifact(id, artifactId);
            audit.success(user.username(), "schema.delete", id + "/" + artifactId, Map.of());
            return successList(id, "Schema '" + artifactId + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.delete", id + "/" + artifactId, reason);
            return errorDetail(id, artifactId, "Delete failed: " + reason);
        }
    }

    /* helpers */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    static String describe(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause != cause.getCause()) cause = cause.getCause();
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        return msg == null || msg.isBlank() ? type : type + ": " + msg;
    }

    private static Response successList(String id, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/schemas?success=" + enc(msg))).build();
    }

    private static Response errorList(String id, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/schemas?error=" + enc(msg))).build();
    }

    private static Response successDetail(String id, String artifactId, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/schemas/" + artifactId + "?success=" + enc(msg))).build();
    }

    private static Response errorDetail(String id, String artifactId, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/schemas/" + artifactId + "?error=" + enc(msg))).build();
    }

    private static String enc(String s) {
        return s == null ? "" : URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
