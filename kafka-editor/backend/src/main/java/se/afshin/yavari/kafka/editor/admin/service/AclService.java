package se.afshin.yavari.kafka.editor.admin.service;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.AdminErrors;
import se.afshin.yavari.kafka.editor.admin.dto.AclEntry;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Native Kafka ACLs. On a broker without an authorizer these calls fail with
 * SecurityDisabledException — translated to an UNSUPPORTED {@code AdminApiException}
 * so the resource can report a capability rather than a 500.
 */
@ApplicationScoped
public class AclService {

    @Inject
    AdminClientFactory factory;

    public List<AclEntry> list(ConnectionConfig conn) {
        Admin admin = factory.admin(conn);
        Collection<AclBinding> bindings =
                AdminErrors.await(admin.describeAcls(AclBindingFilter.ANY).values());
        return bindings.stream()
                .map(AclService::toEntry)
                .sorted(Comparator.comparing(AclEntry::resourceType)
                        .thenComparing(AclEntry::resourceName)
                        .thenComparing(AclEntry::principal))
                .toList();
    }

    public void create(ConnectionConfig conn, AclEntry entry) {
        AdminErrors.await(factory.admin(conn)
                .createAcls(List.of(toBinding(entry))).all());
    }

    /** Delete every ACL matching the entry; returns how many were removed. */
    public int delete(ConnectionConfig conn, AclEntry entry) {
        Collection<AclBinding> deleted = AdminErrors.await(factory.admin(conn)
                .deleteAcls(List.of(toFilter(entry))).all());
        return deleted.size();
    }

    static AclEntry toEntry(AclBinding binding) {
        return new AclEntry(
                binding.pattern().resourceType().name(),
                binding.pattern().name(),
                binding.pattern().patternType().name(),
                binding.entry().principal(),
                binding.entry().host(),
                binding.entry().operation().name(),
                binding.entry().permissionType().name());
    }

    static AclBinding toBinding(AclEntry entry) {
        ResourcePattern pattern = new ResourcePattern(
                ResourceType.valueOf(upper(entry.resourceType())),
                entry.resourceName(),
                PatternType.valueOf(upperOr(entry.patternType(), "LITERAL")));
        AccessControlEntry ace = new AccessControlEntry(
                entry.principal(),
                entry.host() == null || entry.host().isBlank()
                        ? "*" : entry.host(),
                AclOperation.valueOf(upper(entry.operation())),
                AclPermissionType.valueOf(upperOr(entry.permissionType(), "ALLOW")));
        return new AclBinding(pattern, ace);
    }

    static AclBindingFilter toFilter(AclEntry entry) {
        ResourcePatternFilter pattern = new ResourcePatternFilter(
                ResourceType.valueOf(upper(entry.resourceType())),
                entry.resourceName(),
                PatternType.valueOf(upperOr(entry.patternType(), "LITERAL")));
        AccessControlEntryFilter ace = new AccessControlEntryFilter(
                entry.principal(),
                entry.host() == null || entry.host().isBlank()
                        ? "*" : entry.host(),
                AclOperation.valueOf(upper(entry.operation())),
                AclPermissionType.valueOf(upperOr(entry.permissionType(), "ALLOW")));
        return new AclBindingFilter(pattern, ace);
    }

    private static String upper(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("A required ACL field is missing.");
        }
        return value.trim().toUpperCase();
    }

    private static String upperOr(String value, String fallback) {
        return value == null || value.isBlank()
                ? fallback : value.trim().toUpperCase();
    }
}
