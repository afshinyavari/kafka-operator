package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.afshin.yavari.kafka.ui.audit.AuditLog;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.service.SchemaService;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaContent;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaSummary;

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
    @Inject Toasts toasts;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<SchemaSummary> schemas,
                                                   String username);
        public static native TemplateInstance list$body(ClusterCoordinates cluster,
                                                        List<SchemaSummary> schemas,
                                                        String username);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     SchemaContent schema,
                                                     String username);
        public static native TemplateInstance detail$body(ClusterCoordinates cluster,
                                                          SchemaContent schema,
                                                          String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @HeaderParam("HX-Request") String hx) {
        ClusterCoordinates c = cluster(id);
        List<SchemaSummary> list = schemas.list(id, user.rbac(c.namespace()));
        return "true".equals(hx)
                ? Templates.list$body(c, list, user.username())
                : Templates.list(c, list, user.username());
    }

    @GET
    @Path("/{artifactId}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                                   @HeaderParam("HX-Request") String hx) {
        ClusterCoordinates c = cluster(id);
        SchemaContent s = schemas.get(id, artifactId, user.rbac(c.namespace()));
        return "true".equals(hx)
                ? Templates.detail$body(c, s, user.username())
                : Templates.detail(c, s, user.username());
    }

    @POST
    @Produces(MediaType.TEXT_HTML)
    public Response create(@PathParam("id") String id,
                           @FormParam("artifactId") String artifactId,
                           @FormParam("type") String type,
                           @FormParam("content") String content) {
        ClusterCoordinates c = cluster(id);
        String upperType = type == null ? "" : type.toUpperCase();
        if (artifactId == null || artifactId.isBlank()) {
            return toasts.error("Artifact ID is required.");
        }
        if (!ALLOWED_TYPES.contains(upperType)) {
            return toasts.error("Type must be one of: " + ALLOWED_TYPES + ".");
        }
        try {
            schemas.createArtifact(id, artifactId, upperType, content);
            audit.success(user.username(), "schema.create", id + "/" + artifactId, Map.of("type", upperType));
            return refreshedList(c, "Schema '" + artifactId + "' created.");
        } catch (IllegalArgumentException e) {
            return toasts.error(e.getMessage());
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.create", id + "/" + artifactId, reason);
            return toasts.error("Create failed: " + reason);
        }
    }

    @POST
    @Path("/{artifactId}/versions")
    @Produces(MediaType.TEXT_HTML)
    public Response newVersion(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                               @FormParam("type") String type,
                               @FormParam("content") String content) {
        ClusterCoordinates c = cluster(id);
        String upperType = type == null ? "" : type.toUpperCase();
        if (!ALLOWED_TYPES.contains(upperType)) {
            return toasts.error("Type must be one of: " + ALLOWED_TYPES + ".");
        }
        try {
            schemas.updateArtifact(id, artifactId, upperType, content);
            audit.success(user.username(), "schema.newVersion", id + "/" + artifactId, Map.of("type", upperType));
            SchemaContent updated = schemas.get(id, artifactId, user.rbac(c.namespace()));
            String body = Templates.detail$body(c, updated, user.username()).render();
            return toasts.success(body, "New version of '" + artifactId + "' published.");
        } catch (IllegalArgumentException e) {
            return toasts.error(e.getMessage());
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.newVersion", id + "/" + artifactId, reason);
            return toasts.error("Update failed: " + reason);
        }
    }

    @POST
    @Path("/{artifactId}/delete")
    @Produces(MediaType.TEXT_HTML)
    public Response delete(@PathParam("id") String id, @PathParam("artifactId") String artifactId,
                           @FormParam("confirm") String confirm) {
        ClusterCoordinates c = cluster(id);
        if (!artifactId.equals(confirm)) {
            return toasts.error("Delete cancelled: confirmation did not match.");
        }
        try {
            schemas.deleteArtifact(id, artifactId);
            audit.success(user.username(), "schema.delete", id + "/" + artifactId, Map.of());
            return refreshedListPushUrl(c, "Schema '" + artifactId + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "schema.delete", id + "/" + artifactId, reason);
            return toasts.error("Delete failed: " + reason);
        }
    }

    /* helpers */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    private Response refreshedList(ClusterCoordinates c, String msg) {
        List<SchemaSummary> list = schemas.list(c.id(), user.rbac(c.namespace()));
        String body = Templates.list$body(c, list, user.username()).render();
        return toasts.success(body, msg);
    }

    private Response refreshedListPushUrl(ClusterCoordinates c, String msg) {
        List<SchemaSummary> list = schemas.list(c.id(), user.rbac(c.namespace()));
        String body = Templates.list$body(c, list, user.username()).render();
        return toasts.successPushUrl(body, msg, "/clusters/" + c.id() + "/schemas");
    }

    static String describe(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause != cause.getCause()) cause = cause.getCause();
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        return msg == null || msg.isBlank() ? type : type + ": " + msg;
    }
}
