package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryOidcConfig;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistrySpec;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioProxyContainerBuilderTest {

    private final ApicurioProxyContainerBuilder builder = new ApicurioProxyContainerBuilder();

    @Test
    void talksToRegistryOverLoopback() {
        Container c = builder.build(registry(null));

        var env = envMap(c);
        assertThat(env.get("PROXY_APICURIO_URL")).isEqualTo("http://localhost:8080");
        assertThat(env.get("PROXY_XML_SCHEMA_URL")).isEqualTo("http://localhost:8080");
        assertThat(env.get("PROXY_POLICY_FILE")).isEqualTo("/opt/rbac/policy.yaml");
    }

    @Test
    void containerHasRbacProxyShape() {
        Container c = builder.build(registry(null));

        assertThat(c.getName()).isEqualTo("rbac-proxy");
        assertThat(c.getImage()).isEqualTo("proxy:latest");
        assertThat(c.getPorts()).anyMatch(p -> p.getContainerPort() == ApicurioProxyContainerBuilder.PROXY_PORT);
        assertThat(c.getVolumeMounts())
                .anyMatch(m -> "policy".equals(m.getName()) && "/opt/rbac".equals(m.getMountPath()));
        assertThat(c.getReadinessProbe()).isNotNull();
    }

    @Test
    void appliesResourceRequestsAndLimits() {
        Container c = builder.build(registry(null));

        var req = c.getResources().getRequests();
        var lim = c.getResources().getLimits();
        assertThat(req.get("cpu")).isEqualTo(Quantity.parse("50m"));
        assertThat(req.get("memory")).isEqualTo(Quantity.parse("128Mi"));
        assertThat(lim.get("cpu")).isEqualTo(Quantity.parse("200m"));
        assertThat(lim.get("memory")).isEqualTo(Quantity.parse("256Mi"));
    }

    @Test
    void setsCgroupAwareHeapViaJavaToolOptions() {
        Container c = builder.build(registry(null));

        assertThat(envMap(c).get("JAVA_TOOL_OPTIONS"))
                .contains("MaxRAMPercentage")
                .contains("InitialRAMPercentage");
    }

    @Test
    void oidcConfig_addsQuarkusEnvVars() {
        ApicurioRegistry r = registry(null);
        ApicurioRegistryOidcConfig oidc = new ApicurioRegistryOidcConfig();
        oidc.setIssuerUrl("https://keycloak/realms/k");
        oidc.setGroupsClaim("resource.access.client.roles");
        r.getSpec().setOidc(oidc);

        Container c = builder.build(r);
        var env = envMap(c);

        assertThat(env.get("QUARKUS_OIDC_AUTH_SERVER_URL")).isEqualTo("https://keycloak/realms/k");
        // Quarkus needs "/"-separated nested claim paths
        assertThat(env.get("QUARKUS_OIDC_ROLES_ROLE_CLAIM_PATH")).isEqualTo("resource/access/client/roles");
    }

    @Test
    void oidcConfig_nullGroupsClaim_omitsRoleClaimPath() {
        ApicurioRegistry r = registry(null);
        ApicurioRegistryOidcConfig oidc = new ApicurioRegistryOidcConfig();
        oidc.setIssuerUrl("https://keycloak/realms/k");
        oidc.setGroupsClaim(null);  // override the CRD default
        r.getSpec().setOidc(oidc);

        Container c = builder.build(r);
        assertThat(envMap(c)).doesNotContainKey("QUARKUS_OIDC_ROLES_ROLE_CLAIM_PATH");
    }

    @Test
    void policyVolume_referencesPolicyConfigMap() {
        Volume v = builder.policyVolume("my-rbac");

        assertThat(v.getName()).isEqualTo("policy");
        assertThat(v.getConfigMap().getName()).isEqualTo("my-rbac-apicurio-policy");
    }

    // --- helpers ---

    private ApicurioRegistry registry(String ignoredForNow) {
        ApicurioRegistry r = new ApicurioRegistry();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("apicurio");
        meta.setNamespace("kafka");
        r.setMetadata(meta);
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        spec.setRbacProxyImage("proxy:latest");
        spec.setReplicas(1);
        r.setSpec(spec);
        return r;
    }

    private static java.util.Map<String, String> envMap(Container c) {
        return c.getEnv().stream()
                .filter(e -> e.getValue() != null)
                .collect(java.util.stream.Collectors.toMap(
                        io.fabric8.kubernetes.api.model.EnvVar::getName,
                        io.fabric8.kubernetes.api.model.EnvVar::getValue));
    }
}
