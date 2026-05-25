package se.afshin.yavari.kafka.editor.api;

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

/**
 * Bootstrap-time configuration the SPA reads on load to decide whether to
 * show its standalone cluster-manager UI. Unauthenticated by design — there
 * are no secrets in the payload, only the flag that says "this backend is
 * operator-deployed, the proxy bootstrap is already injected, don't ask the
 * user to type one."
 */
@PermitAll
@Path("/api/config")
public class ConfigResource {

    @Inject
    @ConfigProperty(name = "kafka-editor.bootstrap-servers")
    Optional<String> injectedBootstrap;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Config get() {
        boolean operatorMode = injectedBootstrap.filter(s -> !s.isBlank()).isPresent();
        return new Config(operatorMode);
    }

    /** Returned to the SPA at /api/config. */
    public record Config(boolean operatorMode) {}
}
