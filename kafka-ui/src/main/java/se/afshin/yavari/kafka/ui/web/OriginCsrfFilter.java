package se.afshin.yavari.kafka.ui.web;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.net.URI;
import java.util.Optional;
import java.util.Set;

/**
 * Rejects state-changing requests whose Origin/Referer header does not match
 * the request's Host. Defence-in-depth against CSRF; the primary protection
 * is Quarkus OIDC's SameSite=Lax session cookie, which already blocks the
 * canonical cookie-replay CSRF attack on cross-site POSTs.
 *
 * Disabled by default in test profile (no Origin header sent by RestAssured).
 * Set {@code kafka-ui.csrf.enabled=false} to opt out at runtime.
 */
@Provider
@Priority(Priorities.AUTHENTICATION - 100)
public class OriginCsrfFilter implements ContainerRequestFilter {

    private static final Set<String> STATE_CHANGING = Set.of("POST", "PUT", "PATCH", "DELETE");

    @ConfigProperty(name = "kafka-ui.csrf.enabled", defaultValue = "true")
    boolean enabled;

    /** Comma-separated additional origins allowed (e.g. for an embedded UI). */
    @ConfigProperty(name = "kafka-ui.csrf.allowed-origins")
    Optional<String> allowedOriginsRaw;

    @Override
    public void filter(ContainerRequestContext ctx) {
        if (!enabled) return;
        if (!STATE_CHANGING.contains(ctx.getMethod())) return;

        String origin = firstNonNull(
                ctx.getHeaderString("Origin"),
                ctx.getHeaderString("Referer"));
        if (origin == null || origin.isBlank()) {
            // No origin header at all; modern browsers strip Referer on some
            // navigations but always send Origin on cross-site fetch. Treat as
            // same-origin and rely on SameSite cookie protection.
            return;
        }
        String host = ctx.getHeaderString("Host");
        if (host == null) {
            // Misformed request — reject.
            reject(ctx, "missing Host header");
            return;
        }

        if (originMatchesHost(origin, host)) return;
        if (originInAllowList(origin)) return;

        reject(ctx, "Origin '" + origin + "' does not match Host '" + host + "'");
    }

    private static boolean originMatchesHost(String origin, String host) {
        try {
            URI u = URI.create(origin);
            if (u.getHost() == null) return false;
            String originHostWithPort = u.getHost() + (u.getPort() > 0 ? ":" + u.getPort() : "");
            return host.equals(originHostWithPort) || host.equals(u.getHost());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean originInAllowList(String origin) {
        if (allowedOriginsRaw == null || allowedOriginsRaw.isEmpty()
                || allowedOriginsRaw.get().isBlank()) return false;
        for (String allowed : allowedOriginsRaw.get().split(",")) {
            if (origin.equals(allowed.trim())) return true;
        }
        return false;
    }

    private static void reject(ContainerRequestContext ctx, String reason) {
        ctx.abortWith(Response.status(Response.Status.FORBIDDEN)
                .type("text/plain")
                .entity("CSRF: " + reason)
                .build());
    }

    private static String firstNonNull(String a, String b) {
        return a != null ? a : b;
    }

    /* Testing seam — package-private setter for unit tests that build the
     * filter directly instead of via CDI. */
    void configure(boolean enabled, String allowedOrigins) {
        this.enabled = enabled;
        this.allowedOriginsRaw = Optional.ofNullable(allowedOrigins);
    }
}
