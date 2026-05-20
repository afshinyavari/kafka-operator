package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ApicurioDeploymentBuilder {

    public static final int REGISTRY_PORT = 8080;
    private static final String JAVA_OPTS =
            "-XX:MaxRAMPercentage=50.0 -XX:InitialRAMPercentage=50.0";

    public Deployment build(ApicurioRegistry registry, String namespace,
                             Container proxyContainer, Volume policyVolume) {
        String name = registry.getMetadata().getName();
        Map<String, String> labels = labels(name);

        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(new EnvVarBuilder()
                .withName("JAVA_TOOL_OPTIONS")
                .withValue(JAVA_OPTS)
                .build());

        ApicurioRegistryStorageConfig storage = registry.getSpec().getStorage();
        if (storage != null && "postgresql".equals(storage.getType())) {
            envVars.add(new EnvVarBuilder()
                    .withName("QUARKUS_DATASOURCE_JDBC_URL")
                    .withValue(storage.getJdbcUrl())
                    .build());
            if (storage.getJdbcSecretRef() != null) {
                envVars.add(new EnvVarBuilder()
                        .withName("QUARKUS_DATASOURCE_USERNAME")
                        .withValueFrom(new EnvVarSourceBuilder()
                                .withSecretKeyRef(new SecretKeySelectorBuilder()
                                        .withName(storage.getJdbcSecretRef())
                                        .withKey("username")
                                        .build())
                                .build())
                        .build());
                envVars.add(new EnvVarBuilder()
                        .withName("QUARKUS_DATASOURCE_PASSWORD")
                        .withValueFrom(new EnvVarSourceBuilder()
                                .withSecretKeyRef(new SecretKeySelectorBuilder()
                                        .withName(storage.getJdbcSecretRef())
                                        .withKey("password")
                                        .build())
                                .build())
                        .build());
            }
        }

        Container registryContainer = new ContainerBuilder()
                .withName("registry")
                .withImage(registry.getSpec().getImage())
                .addNewPort()
                    .withName("http")
                    .withContainerPort(REGISTRY_PORT)
                .endPort()
                .withEnv(envVars)
                .withResources(new ResourceRequirementsBuilder()
                        .withRequests(Map.of(
                                "cpu", Quantity.parse("100m"),
                                "memory", Quantity.parse("256Mi")))
                        .withLimits(Map.of(
                                "cpu", Quantity.parse("500m"),
                                "memory", Quantity.parse("512Mi")))
                        .build())
                .withNewReadinessProbe()
                    .withNewHttpGet()
                        .withPath("/health/ready")
                        .withNewPort(REGISTRY_PORT)
                    .endHttpGet()
                    .withInitialDelaySeconds(10)
                    .withPeriodSeconds(10)
                .endReadinessProbe()
                .build();

        List<Container> containers = new ArrayList<>();
        containers.add(registryContainer);
        if (proxyContainer != null) {
            containers.add(proxyContainer);
        }

        List<Volume> volumes = new ArrayList<>();
        if (policyVolume != null) {
            volumes.add(policyVolume);
        }

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name + "-registry")
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
                            .withContainers(containers)
                            .withVolumes(volumes)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "apicurio-registry",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
