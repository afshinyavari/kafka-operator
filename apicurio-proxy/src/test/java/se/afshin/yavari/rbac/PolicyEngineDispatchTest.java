package se.afshin.yavari.rbac;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

/** Either/or dispatch: OIDC identities → role file, certificate identities → Kafka ACLs. */
class PolicyEngineDispatchTest {

    private PolicyEngine engine;
    private KafkaAclPolicySource acls;

    @BeforeEach
    void setUp() throws Exception {
        Path policy = Files.createTempFile("policy", ".yaml");
        Files.writeString(policy, """
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders-value
                    actions: [READ, WRITE]
            """);
        engine = new PolicyEngine();
        engine.reload(policy);
        acls = new KafkaAclPolicySource(() -> List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:CN=orders-service", "*", AclOperation.WRITE, AclPermissionType.ALLOW))),
                List.of("-value", "-key"));
        acls.refresh();
        engine.acls = acls;
        engine.principalMode = MtlsPrincipal.Mode.DN;
    }

    private static SecurityIdentity oidc(String user, String... roles) {
        return QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal(user)).addRoles(Set.of(roles)).build();
    }

    private static SecurityIdentity mtls(String dn) {
        return QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal(dn))
                .addAttribute(MtlsPrincipal.AUTH_ATTRIBUTE, MtlsPrincipal.AUTH_MTLS).build();
    }

    @Test
    void oidcIdentityUsesRoleRules() {
        assertThat(engine.isAllowed(oidc("alice", "orders-team"), "orders-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(oidc("alice", "orders-team"), "payments-value", READ)).isFalse();
    }

    @Test
    void oidcIdentityNeverConsultsAcls() {
        // "CN=orders-service" as an OIDC user with no roles: ACLs would allow, roles don't.
        assertThat(engine.isAllowed(oidc("CN=orders-service"), "orders-value", WRITE)).isFalse();
    }

    @Test
    void mtlsIdentityUsesKafkaAcls() {
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-key", READ)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "payments-value", READ)).isFalse();
    }

    @Test
    void mtlsIdentityNeverConsultsRoleFile() {
        SecurityIdentity withRole = QuarkusSecurityIdentity.builder()
                .setPrincipal(new QuarkusPrincipal("CN=nobody")).addRole("orders-team")
                .addAttribute(MtlsPrincipal.AUTH_ATTRIBUTE, MtlsPrincipal.AUTH_MTLS).build();
        assertThat(engine.isAllowed(withRole, "orders-value", WRITE)).isFalse();
    }

    @Test
    void mtlsIdentityDeniedWhenAclSourceDisabled() {
        engine.acls = new KafkaAclPolicySource(null, List.of("-value"));
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-value", WRITE)).isFalse();
    }

    @Test
    void mtlsPrincipalRespectsCnMode() {
        engine.principalMode = MtlsPrincipal.Mode.CN;
        acls.replaceSnapshot(List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:orders-service", "*", AclOperation.READ, AclPermissionType.ALLOW))));
        assertThat(engine.isAllowed(mtls("CN=orders-service,O=Acme"), "orders-value", READ)).isTrue();
        assertThat(PolicyEngine.principalOf(mtls("CN=orders-service,O=Acme"), MtlsPrincipal.Mode.CN))
                .isEqualTo("user:orders-service");
        assertThat(PolicyEngine.principalOf(oidc("alice"), MtlsPrincipal.Mode.CN)).isEqualTo("user:alice");
        assertThat(PolicyEngine.principalOf(null, MtlsPrincipal.Mode.DN)).isEqualTo("anonymous");
    }
}
