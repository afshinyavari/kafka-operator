package se.afshin.yavari.kafka.editor.audit;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Auto-instruments every authenticated mutating call (POST/PUT/PATCH/DELETE)
 * with one line on the {@code kafka-editor.audit} channel — principal, HTTP
 * method, request path, status code. Read calls (GET/HEAD) are excluded so we
 * don't drown the audit log in topic-browse noise; per-resource read audit
 * lives at the Kroxylicious / proxy layer (project_unified_audit_logging).
 */
@Provider
@Priority(Priorities.USER)
public class AuditFilter implements ContainerResponseFilter {

    @Inject AuditLog audit;

    @Inject Instance<SecurityIdentity> identity;

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext resp) {
        String method = req.getMethod();
        if (method == null) return;
        switch (method.toUpperCase()) {
            case "POST", "PUT", "PATCH", "DELETE" -> {}
            default -> { return; }
        }

        String user = identity != null && !identity.isUnsatisfied() && !identity.get().isAnonymous()
                ? identity.get().getPrincipal().getName()
                : "anonymous";
        String path = req.getUriInfo().getPath();
        int status = resp.getStatus();
        String outcome = status >= 200 && status < 400 ? "success" : "failure";

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("method", method);
        details.put("status", status);

        if ("success".equals(outcome)) {
            audit.success(user, "http." + method.toLowerCase(), path, details);
        } else {
            audit.failure(user, "http." + method.toLowerCase(), path,
                    "HTTP " + status);
        }
    }
}
