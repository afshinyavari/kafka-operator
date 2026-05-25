package se.afshin.yavari.kafka.editor.rbac;

import io.quarkus.security.Authenticated;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;
import java.util.Set;

/**
 * Self-introspection for the authenticated user. The SPA calls this once on
 * load to know which topics / schemas it can show and which buttons to
 * disable — final enforcement still happens at the proxy.
 */
@Authenticated
@Path("/api/me")
public class MeResource {

    @Inject SecurityIdentity identity;
    @Inject RbacRulesService rbac;

    /** Operator-injected: the Kubernetes namespace the kafka-editor pod runs in. */
    @Inject
    @ConfigProperty(name = "kafka-editor.namespace")
    Optional<String> namespace;

    @GET
    @Path("/rbac")
    @Produces(MediaType.APPLICATION_JSON)
    public UserRbac rbac() {
        Set<String> groups = identity.getRoles();
        return rbac.forUser(namespace.orElse("kafka"), groups);
    }
}
