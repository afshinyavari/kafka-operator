package se.afshin.yavari.kafka.ui.web;

import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.ui.rbac.RbacRulesService;
import se.afshin.yavari.kafka.ui.rbac.UserRbac;

import java.util.Set;

/**
 * Resolves the current user's groups from {@link SecurityIdentity} and caches
 * the {@link UserRbac} view per request so multiple page sections can reuse it.
 */
@RequestScoped
public class UserContext {

    @Inject SecurityIdentity identity;
    @Inject RbacRulesService rbacService;

    private UserRbac cached;
    private String cachedNamespace;

    public String username() {
        return identity.getPrincipal() == null ? "anonymous" : identity.getPrincipal().getName();
    }

    public Set<String> groups() {
        return identity.getRoles();
    }

    public UserRbac rbac(String namespace) {
        if (cached != null && namespace.equals(cachedNamespace)) return cached;
        cachedNamespace = namespace;
        cached = rbacService.forUser(namespace, groups());
        return cached;
    }
}
