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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

/** Dispatch: OIDC identities → role file; certificate identities → Kafka ACLs, plus role
 *  rules for roles mapped to the certificate principal in the policy file's {@code principals}. */
class PolicyEngineDispatchTest {

    private PolicyEngine engine;
    private KafkaAclPolicySource acls;

    @BeforeEach
    void setUp() throws Exception {
        engine = new PolicyEngine();
        engine.load("""
            principals:
              "CN=payments-app": [payments-writer]
              "CN=ci-pipeline, O=Acme": [schema-admin]
              "User:CN=kafka-style": [payments-writer]
              "orders-service": [payments-writer]
            rules:
              - roles: [orders-team]
                resources:
                  - artifact: orders-value
                    actions: [READ, WRITE]
              - roles: [payments-writer]
                resources:
                  - artifact: payments-value
                    actions: [READ, WRITE]
              - roles: [schema-admin]
                resources:
                  - artifact: "*"
                    actions: [READ, WRITE, DELETE]
            """.getBytes(StandardCharsets.UTF_8));
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
    void mtlsIdentityIgnoresRolesCarriedOnTheIdentity() {
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

    // ── principals: certificate → roles ───────────────────────────────────────

    @Test
    void mappedPrincipalGetsRoleRulesWithoutAnyAcl() {
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", DELETE)).isFalse();
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "orders-value", READ)).isFalse();
    }

    @Test
    void mappingAddsToKafkaAcls() {
        // orders-service: WRITE on topic orders from Kafka; its CN-mode mapping is not active in DN mode
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-value", WRITE)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "payments-value", READ)).isFalse();
    }

    @Test
    void mappingKeysAreNormalizedDnsWithOptionalUserPrefix() {
        assertThat(engine.isAllowed(mtls("CN=ci-pipeline,O=Acme"), "anything", DELETE)).isTrue();
        assertThat(engine.isAllowed(mtls("CN=kafka-style"), "payments-value", WRITE)).isTrue();
    }

    @Test
    void mappingKeysMatchBareCnInCnMode() {
        engine.principalMode = MtlsPrincipal.Mode.CN;
        assertThat(engine.isAllowed(mtls("CN=orders-service,O=Acme"), "payments-value", WRITE)).isTrue();
    }

    @Test
    void mappingNeverAppliesToOidcIdentities() {
        assertThat(engine.isAllowed(oidc("CN=payments-app"), "payments-value", READ)).isFalse();
    }

    @Test
    void kafkaDenyVetoesPolicyGrant() {
        acls.replaceSnapshot(List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "payments", PatternType.LITERAL),
                new AccessControlEntry("User:CN=payments-app", "*", AclOperation.WRITE, AclPermissionType.DENY))));
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isFalse();
        // the DENY targets WRITE only
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", READ)).isTrue();
    }

    @Test
    void mappingWorksWhenAclSourceDisabled() {
        engine.acls = new KafkaAclPolicySource(null, List.of("-value"));
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isTrue();
    }

    @Test
    void mappingDeniedWhileEnabledAclSourceHasNoSnapshot() {
        // Enabled but never loaded: a Kafka DENY cannot be ruled out, so fail closed.
        KafkaAclPolicySource failing = new KafkaAclPolicySource(() -> {
            throw new IllegalStateException("kafka down");
        }, List.of("-value"));
        failing.refresh();
        engine.acls = failing;
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isFalse();
    }

    @Test
    void reloadReplacesMappings() throws Exception {
        engine.load("""
            rules:
              - roles: [payments-writer]
                resources:
                  - artifact: payments-value
                    actions: [READ, WRITE]
            """.getBytes(StandardCharsets.UTF_8));
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isFalse();
    }

    @Test
    void firstAllowedPicksTheArtifactTheIdentityMayUse() {
        // One content id shared by several artifacts: access to any of them is enough.
        assertThat(engine.firstAllowed(mtls("CN=orders-service"), List.of("payments-key", "orders-key"), READ))
                .contains("orders-key");
        assertThat(engine.firstAllowed(oidc("alice", "orders-team"), List.of("orders-value", "payments-value"), READ))
                .contains("orders-value");
    }

    @Test
    void firstAllowedIsEmptyWhenNoArtifactIsPermitted() {
        assertThat(engine.firstAllowed(mtls("CN=orders-service"), List.of("payments-key", "invoices-key"), READ))
                .isEmpty();
        assertThat(engine.firstAllowed(mtls("CN=orders-service"), List.of(), READ)).isEmpty();
    }

    private static final String CN_ONLY_RULES = "RULE:^CN=([^,]+),.*$/CN=$1/,DEFAULT";

    @Test
    void mappingRulesDecideTheAclPrincipal() {
        // The ACL is on User:CN=orders-service; the certificate also carries an organization.
        assertThat(engine.isAllowed(mtls("CN=orders-service,O=Acme"), "orders-value", WRITE)).isFalse();
        engine.principalMappingRules = Optional.of(CN_ONLY_RULES);
        assertThat(engine.isAllowed(mtls("CN=orders-service,O=Acme"), "orders-value", WRITE)).isTrue();
    }

    @Test
    void mappingRulesDecideThePolicyFilePrincipal() {
        engine.principalMappingRules = Optional.of(CN_ONLY_RULES);
        assertThat(engine.isAllowed(mtls("CN=payments-app,O=Acme"), "payments-value", WRITE)).isTrue();
    }

    @Test
    void certificateNoRuleMatchesIsDenied() {
        // In DN mode this certificate gets payments-writer from the policy file.
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isTrue();
        engine.principalMappingRules = Optional.of("RULE:^CN=([^,]+),O=Acme$/CN=$1/");
        assertThat(engine.isAllowed(mtls("CN=payments-app"), "payments-value", WRITE)).isFalse();
    }

    @Test
    void blankMappingRulesFallBackToTheMode() {
        engine.principalMappingRules = Optional.of(" ");
        assertThat(engine.isAllowed(mtls("CN=orders-service"), "orders-value", WRITE)).isTrue();
    }

    @Test
    void auditPrincipalFollowsTheMappingRules() {
        var rules = MtlsPrincipal.Mapping.ofRules("RULE:^CN=([^,]+),O=Acme$/CN=$1/");
        assertThat(PolicyEngine.principalOf(mtls("CN=orders-service,O=Acme"), rules)).isEqualTo("user:CN=orders-service");
        // No rule matches: the audit line still names the certificate.
        assertThat(PolicyEngine.principalOf(mtls("CN=orders-service,O=Other"), rules))
                .isEqualTo("user:CN=orders-service,O=Other");
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
