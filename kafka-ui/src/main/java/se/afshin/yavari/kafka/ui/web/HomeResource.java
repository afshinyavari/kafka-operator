package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;

import java.util.List;

@Path("/")
@Authenticated
public class HomeResource {

    @Inject ClusterRegistry registry;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance home(List<ClusterCoordinates> clusters, String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance home() {
        return Templates.home(registry.list(), user.username());
    }
}
