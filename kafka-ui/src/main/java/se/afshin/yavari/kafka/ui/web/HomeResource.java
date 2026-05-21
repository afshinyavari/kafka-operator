package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
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
        public static native TemplateInstance home$body(List<ClusterCoordinates> clusters, String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance home(@HeaderParam("HX-Request") String hxRequest) {
        var clusters = registry.list();
        var name = user.username();
        return "true".equals(hxRequest)
                ? Templates.home$body(clusters, name)
                : Templates.home(clusters, name);
    }
}
