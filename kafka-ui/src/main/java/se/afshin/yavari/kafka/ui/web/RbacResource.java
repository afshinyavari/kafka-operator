package se.afshin.yavari.kafka.ui.web;

import io.fabric8.kubernetes.client.KubernetesClient;
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
import se.afshin.yavari.kafka.ui.crd.KafkaRbacCr;

import java.util.List;

/**
 * Renders the {@code KafkaRbac} CRs that govern this cluster. Header copy says
 * "Access rules", not "ACLs" — the proxy enforces these, not Kafka's native ACL
 * mechanism. Read-only by design (see docs/security.md → UI Phase 2 trust model).
 */
@Path("/clusters/{id}/acls")
@Authenticated
public class RbacResource {

    @Inject ClusterRegistry registry;
    @Inject KubernetesClient k8s;
    @Inject UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<KafkaRbacCr> rbacs,
                                                   List<String> userGroups,
                                                   String username);
        public static native TemplateInstance list$body(ClusterCoordinates cluster,
                                                        List<KafkaRbacCr> rbacs,
                                                        List<String> userGroups,
                                                        String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @HeaderParam("HX-Request") String hx) {
        ClusterCoordinates c = registry.byId(id)
                .orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
        List<KafkaRbacCr> rbacs = k8s.resources(KafkaRbacCr.class).inNamespace(c.namespace())
                .list().getItems();
        List<String> groups = List.copyOf(user.groups());
        return "true".equals(hx)
                ? Templates.list$body(c, rbacs, groups, user.username())
                : Templates.list(c, rbacs, groups, user.username());
    }
}
