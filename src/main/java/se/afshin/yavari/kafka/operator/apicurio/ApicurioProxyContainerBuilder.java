package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryOidcConfig;

import java.util.Map;

@ApplicationScoped
public class ApicurioProxyContainerBuilder {

    public static final int PROXY_PORT = 8082;
    private static final String POLICY_MOUNT = "/opt/rbac";
    private static final String POLICY_FILE = POLICY_MOUNT + "/policy.yaml";
    private static final String JAVA_OPTS =
            "-XX:MaxRAMPercentage=50.0 -XX:InitialRAMPercentage=50.0";
    private static final String REGISTRY_LOOPBACK =
            "http://localhost:" + ApicurioDeploymentBuilder.REGISTRY_PORT;

    public Container build(ApicurioRegistry registry) {
        ApicurioRegistryOidcConfig oidc = registry.getSpec().getOidc();

        var container = new ContainerBuilder()
                .withName("rbac-proxy")
                .withImage(registry.getSpec().getRbacProxyImage())
                .addNewPort()
                    .withName("http")
                    .withContainerPort(PROXY_PORT)
                .endPort()
                .addToEnv(new EnvVarBuilder()
                        .withName("PROXY_APICURIO_URL")
                        .withValue(REGISTRY_LOOPBACK)
                        .build())
                .addToEnv(new EnvVarBuilder()
                        .withName("PROXY_XML_SCHEMA_URL")
                        .withValue(REGISTRY_LOOPBACK)
                        .build())
                .addToEnv(new EnvVarBuilder()
                        .withName("PROXY_POLICY_FILE")
                        .withValue(POLICY_FILE)
                        .build())
                .addToEnv(new EnvVarBuilder()
                        .withName("JAVA_TOOL_OPTIONS")
                        .withValue(JAVA_OPTS)
                        .build())
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("policy")
                        .withMountPath(POLICY_MOUNT)
                        .build())
                .withResources(new ResourceRequirementsBuilder()
                        .withRequests(Map.of(
                                "cpu", Quantity.parse("50m"),
                                "memory", Quantity.parse("128Mi")))
                        .withLimits(Map.of(
                                "cpu", Quantity.parse("200m"),
                                "memory", Quantity.parse("256Mi")))
                        .build())
                .withNewReadinessProbe()
                    .withNewHttpGet()
                        .withPath("/q/health/ready")
                        .withNewPort(PROXY_PORT)
                    .endHttpGet()
                    .withInitialDelaySeconds(10)
                    .withPeriodSeconds(10)
                .endReadinessProbe();

        if (oidc != null) {
            container.addToEnv(new EnvVarBuilder()
                    .withName("QUARKUS_OIDC_AUTH_SERVER_URL")
                    .withValue(oidc.getIssuerUrl())
                    .build());
            if (oidc.getGroupsClaim() != null) {
                // Quarkus OIDC role-claim-path uses "/" as separator for nested claims
                String claimPath = oidc.getGroupsClaim().replace('.', '/');
                container.addToEnv(new EnvVarBuilder()
                        .withName("QUARKUS_OIDC_ROLES_ROLE_CLAIM_PATH")
                        .withValue(claimPath)
                        .build());
            }
        }

        return container.build();
    }

    public Volume policyVolume(String rbacRef) {
        return new VolumeBuilder()
                .withName("policy")
                .withNewConfigMap()
                    .withName(rbacRef + "-apicurio-policy")
                .endConfigMap()
                .build();
    }
}
