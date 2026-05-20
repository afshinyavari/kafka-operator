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
import se.afshin.yavari.kafka.ui.service.TopicService;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicDetail;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicSummary;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Path("/clusters/{id}/topics")
@Authenticated
public class TopicResource {

    @Inject ClusterRegistry registry;
    @Inject TopicService topicService;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<TopicSummary> topics,
                                                   String username);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     TopicDetail topic,
                                                     String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        return Templates.list(c, topicService.list(id, user.rbac(c.namespace())), user.username());
    }

    @GET
    @Path("/{name}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("name") String name)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        return Templates.detail(c, topicService.describe(id, name, user.rbac(c.namespace())), user.username());
    }

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }
}
