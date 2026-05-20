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
import se.afshin.yavari.kafka.ui.service.GroupService;
import se.afshin.yavari.kafka.ui.service.GroupService.GroupSummary;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Path("/clusters/{id}/groups")
@Authenticated
public class GroupResource {

    @Inject ClusterRegistry registry;
    @Inject GroupService groupService;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<GroupSummary> groups,
                                                   String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = registry.byId(id)
                .orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
        return Templates.list(c, groupService.list(id), user.username());
    }
}
