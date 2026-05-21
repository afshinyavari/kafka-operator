package se.afshin.yavari.kafka.operator.acl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;
import se.afshin.yavari.kafka.operator.topic.KafkaTopicService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Idempotent ACL provisioning for non-super-user principals (e.g. the Apicurio
 * Registry kafkasql client). Uses the operator's existing mTLS identity
 * (resolved via {@link AdminClientTlsLoader}) to talk to the broker.
 *
 * <p>The operator runs as {@code User:CN=kafka-proxy}, listed in broker
 * {@code super.users}, so it has unconditional ACL write access. See
 * {@code ServerPropertiesBuilder} for where {@code authorizer.class.name} and
 * {@code allow.everyone.if.no.acl.found=false} are enabled.
 */
@ApplicationScoped
public class KafkaAclManager {

    private static final Logger LOG = Logger.getLogger(KafkaAclManager.class);
    private static final long TIMEOUT_SECONDS = 10;

    @Inject BrokerBootstrapResolver bootstrapResolver;
    @Inject AdminClientTlsLoader tlsLoader;
    @Inject KafkaTopicService adminFactory;

    /**
     * Creates any of {@code desired} that are not already present on the broker
     * (matched on principal+resource+operation+permissionType+host). Existing ACLs
     * for the same principal that are not in {@code desired} are left untouched —
     * callers manage their own scope.
     */
    public void apply(String clusterName, String namespace, String adminCertSecret,
                      Collection<AclBinding> desired) {
        if (desired == null || desired.isEmpty()) return;
        try (AdminClient admin = newAdmin(clusterName, namespace, adminCertSecret)) {
            Set<AclBinding> existing = describePrincipalAcls(admin, principalsIn(desired));
            List<AclBinding> missing = new ArrayList<>();
            for (AclBinding b : desired) {
                if (!existing.contains(b)) missing.add(b);
            }
            if (missing.isEmpty()) {
                LOG.debugf("ACLs already present for %d binding(s); nothing to create", desired.size());
                return;
            }
            LOG.infof("Creating %d ACL binding(s) on cluster %s/%s", missing.size(), namespace, clusterName);
            admin.createAcls(missing).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AclProvisioningException("Interrupted while applying ACLs", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new AclProvisioningException("Failed to apply ACLs: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes every ACL whose principal equals {@code principal}. Called from the
     * Apicurio reconciler's finalizer when the registry CR is removed.
     */
    public void delete(String clusterName, String namespace, String adminCertSecret, String principal) {
        try (AdminClient admin = newAdmin(clusterName, namespace, adminCertSecret)) {
            AclBindingFilter filter = new AclBindingFilter(
                    ResourcePatternFilter.ANY,
                    new AccessControlEntryFilter(principal, null, org.apache.kafka.common.acl.AclOperation.ANY,
                            org.apache.kafka.common.acl.AclPermissionType.ANY));
            LOG.infof("Deleting all ACLs for principal %s on cluster %s/%s",
                    principal, namespace, clusterName);
            admin.deleteAcls(List.of(filter)).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AclProvisioningException("Interrupted while deleting ACLs", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new AclProvisioningException("Failed to delete ACLs: " + e.getMessage(), e);
        }
    }

    AdminClient newAdmin(String clusterName, String namespace, String adminCertSecret) {
        String bootstrap = bootstrapResolver.resolve(clusterName, namespace);
        Properties sslProps = tlsLoader.loadAsAdminClientSslProps(namespace, adminCertSecret);
        return adminFactory.newAdmin(bootstrap, sslProps);
    }

    private Set<AclBinding> describePrincipalAcls(AdminClient admin, Set<String> principals)
            throws InterruptedException, ExecutionException, TimeoutException {
        Set<AclBinding> all = new HashSet<>();
        for (String principal : principals) {
            AclBindingFilter filter = new AclBindingFilter(
                    ResourcePatternFilter.ANY,
                    new AccessControlEntryFilter(principal, null, org.apache.kafka.common.acl.AclOperation.ANY,
                            org.apache.kafka.common.acl.AclPermissionType.ANY));
            all.addAll(admin.describeAcls(filter).values().get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        return all;
    }

    private static Set<String> principalsIn(Collection<AclBinding> bindings) {
        Set<String> out = new HashSet<>();
        for (AclBinding b : bindings) out.add(b.entry().principal());
        return out;
    }

    public static class AclProvisioningException extends RuntimeException {
        public AclProvisioningException(String message) { super(message); }
        public AclProvisioningException(String message, Throwable cause) { super(message, cause); }
    }
}
