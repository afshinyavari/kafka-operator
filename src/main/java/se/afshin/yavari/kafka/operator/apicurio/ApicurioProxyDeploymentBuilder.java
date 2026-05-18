package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryOidcConfig;

import java.util.Map;

@ApplicationScoped
public class ApicurioProxyDeploymentBuilder {

    public static final int PROXY_PORT = 8082;
    private static final String POLICY_MOUNT = "/opt/rbac";
    private static final String POLICY_FILE = POLICY_MOUNT + "/policy.yaml";

    public Deployment build(ApicurioRegistry registry, String namespace) {
        String name = registry.getMetadata().getName();
        String rbacRef = registry.getSpec().getRbacRef();
        Map<String, String> labels = labels(name);

        String registryUrl = "http://" + name + "-registry." + namespace + ".svc.cluster.local:"
                + ApicurioDeploymentBuilder.REGISTRY_PORT;

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
                        .withValue(registryUrl)
                        .build())
                .addToEnv(new EnvVarBuilder()
                        .withName("PROXY_XML_SCHEMA_URL")
                        .withValue(registryUrl)
                        .build())
                .addToEnv(new EnvVarBuilder()
                        .withName("PROXY_POLICY_FILE")
                        .withValue(POLICY_FILE)
                        .build())
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("policy")
                        .withMountPath(POLICY_MOUNT)
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

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name + "-rbac-proxy")
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(registry.getSpec().getReplicas())
                    .withNewSelector()
                        .withMatchLabels(labels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withContainers(container.build())
                            .withVolumes(new VolumeBuilder()
                                    .withName("policy")
                                    .withNewConfigMap()
                                        .withName(rbacRef + "-apicurio-policy")
                                    .endConfigMap()
                                    .build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "apicurio-rbac-proxy",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
