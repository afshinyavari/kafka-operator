package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.service.ClusterDashboardService;
import se.afshin.yavari.kafka.ui.service.ClusterDashboardService.Dashboard;

import java.util.concurrent.ExecutionException;

@Path("/clusters/{id}")
@Authenticated
public class ClusterResource {

    @Inject ClusterRegistry registry;
    @Inject ClusterDashboardService dashboardService;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance dashboard(ClusterCoordinates cluster,
                                                        Dashboard dashboard,
                                                        String username);
        public static native TemplateInstance dashboard$body(ClusterCoordinates cluster,
                                                             Dashboard dashboard,
                                                             String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance dashboard(@PathParam("id") String id,
                                      @HeaderParam("HX-Request") String hxRequest)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = registry.byId(id)
                .orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
        Dashboard d = dashboardService.load(id);
        String name = user.username();
        return "true".equals(hxRequest)
                ? Templates.dashboard$body(c, d, name)
                : Templates.dashboard(c, d, name);
    }
}
