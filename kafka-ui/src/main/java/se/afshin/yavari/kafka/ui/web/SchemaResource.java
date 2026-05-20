package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.service.SchemaService;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaContent;
import se.afshin.yavari.kafka.ui.service.SchemaService.SchemaSummary;

import java.util.List;

@Path("/clusters/{id}/schemas")
@Authenticated
public class SchemaResource {

    @Inject ClusterRegistry registry;
    @Inject SchemaService schemas;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<SchemaSummary> schemas,
                                                   String username);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     SchemaContent schema,
                                                     String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id) {
        ClusterCoordinates c = cluster(id);
        return Templates.list(c, schemas.list(id, user.rbac(c.namespace())), user.username());
    }

    @GET
    @Path("/{artifactId}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("artifactId") String artifactId) {
        ClusterCoordinates c = cluster(id);
        return Templates.detail(c, schemas.get(id, artifactId, user.rbac(c.namespace())), user.username());
    }

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }
}
