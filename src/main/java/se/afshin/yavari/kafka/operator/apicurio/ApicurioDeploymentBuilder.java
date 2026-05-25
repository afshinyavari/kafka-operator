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
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;
import se.afshin.yavari.kafka.operator.infra.PemToPkcs12InitContainer;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ApicurioDeploymentBuilder {

    public static final int REGISTRY_PORT = 8080;
    public static final String KAFKASQL_TLS_MOUNT = "/etc/kafka/client-tls";
    public static final String KAFKASQL_PKCS12_MOUNT = "/tmp/pkcs12";
    /** PodTemplate annotation carrying the operator's configHash. Flipping it triggers a
     *  rolling restart — the reconciler folds Secret resourceVersions in so cert-manager
     *  rotations propagate to the Apicurio pod. */
    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";
    private static final String KAFKASQL_TLS_VOLUME = "kafkasql-client-tls";
    private static final String KAFKASQL_PKCS12_VOLUME = "kafkasql-pkcs12";
    private static final String KAFKASQL_KEYSTORE_PASS = "changeit";
    private static final String JAVA_OPTS =
            "-XX:MaxRAMPercentage=50.0 -XX:InitialRAMPercentage=50.0";

    /**
     * Resolved kafkasql wiring passed in by the reconciler. {@code tlsSecretRef} is
     * non-null when the target {@link se.afshin.yavari.kafka.operator.crd.KafkaCluster}
     * has {@code proxyMtls.enabled=true}. {@code initImage} is the Kafka container image
     * (which carries {@code openssl} + {@code keytool}); used by the PEM→PKCS12 init
     * container when TLS is enabled, because Apicurio's Kafka client config only accepts
     * keystore-file paths (PKCS12/JKS), not inline PEM material.
     */
    public record KafkasqlConfig(String bootstrapServers, String topic, String tlsSecretRef,
                                  String initImage) {}

    public Deployment build(ApicurioRegistry registry, String namespace,
                             Container proxyContainer, Volume policyVolume,
                             KafkasqlConfig kafkasql, String configHash) {
        String name = registry.getMetadata().getName();
        Map<String, String> labels = labels(name);

        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(new EnvVarBuilder()
                .withName("JAVA_TOOL_OPTIONS")
                .withValue(JAVA_OPTS)
                .build());

        List<VolumeMount> registryVolumeMounts = new ArrayList<>();
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
        } else if (storage != null && "kafkasql".equals(storage.getType()) && kafkasql != null) {
            // Apicurio v2.6 exposes Kafka client config via KAFKA_* env vars (see
            // registry-kafkasql application.properties: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
            // KAFKA_SECURITY_PROTOCOL, KAFKA_SSL_{KEYSTORE,TRUSTSTORE}_*). Apicurio's
            // kafkasql ssl.keystore.location only takes a keystore file path (PKCS12/JKS),
            // hence the PEM→PKCS12 init container below.
            envVars.add(envVar("KAFKA_BOOTSTRAP_SERVERS", kafkasql.bootstrapServers()));
            envVars.add(envVar("KAFKA_TOPIC", kafkasql.topic()));
            if (kafkasql.tlsSecretRef() != null) {
                envVars.add(envVar("KAFKA_SECURITY_PROTOCOL", "SSL"));
                envVars.add(envVar("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12"));
                envVars.add(envVar("KAFKA_SSL_KEYSTORE_LOCATION", KAFKASQL_PKCS12_MOUNT + "/keystore.p12"));
                envVars.add(envVar("KAFKA_SSL_KEYSTORE_PASSWORD", KAFKASQL_KEYSTORE_PASS));
                envVars.add(envVar("KAFKA_SSL_KEY_PASSWORD", KAFKASQL_KEYSTORE_PASS));
                envVars.add(envVar("KAFKA_SSL_TRUSTSTORE_TYPE", "PKCS12"));
                envVars.add(envVar("KAFKA_SSL_TRUSTSTORE_LOCATION", KAFKASQL_PKCS12_MOUNT + "/truststore.p12"));
                envVars.add(envVar("KAFKA_SSL_TRUSTSTORE_PASSWORD", KAFKASQL_KEYSTORE_PASS));
                registryVolumeMounts.add(new VolumeMountBuilder()
                        .withName(KAFKASQL_PKCS12_VOLUME)
                        .withMountPath(KAFKASQL_PKCS12_MOUNT)
                        .withReadOnly(true)
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
                .withVolumeMounts(registryVolumeMounts)
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
                    .withFailureThreshold(6)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withNewHttpGet()
                        .withPath("/health/live")
                        .withNewPort(REGISTRY_PORT)
                    .endHttpGet()
                    .withInitialDelaySeconds(30)
                    .withPeriodSeconds(20)
                    .withFailureThreshold(3)
                .endLivenessProbe()
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();

        List<Container> containers = new ArrayList<>();
        containers.add(registryContainer);
        if (proxyContainer != null) {
            containers.add(proxyContainer);
        }

        List<Container> initContainers = new ArrayList<>();
        List<Volume> volumes = new ArrayList<>();
        if (policyVolume != null) {
            volumes.add(policyVolume);
        }
        if (storage != null && "kafkasql".equals(storage.getType())
                && kafkasql != null && kafkasql.tlsSecretRef() != null) {
            // PEM (cert-manager Secret) → PKCS12 (what Apicurio's Kafka client config
            // expects via *_LOCATION env vars). Done in an initContainer that has
            // openssl + keytool — both are present in the Kafka image.
            volumes.add(new VolumeBuilder()
                    .withName(KAFKASQL_TLS_VOLUME)
                    .withNewSecret()
                        .withSecretName(kafkasql.tlsSecretRef())
                    .endSecret()
                    .build());
            volumes.add(new VolumeBuilder()
                    .withName(KAFKASQL_PKCS12_VOLUME)
                    .withNewEmptyDir().endEmptyDir()
                    .build());
            initContainers.add(PemToPkcs12InitContainer.build(
                    "pem-to-pkcs12",
                    kafkasql.initImage(),
                    KAFKASQL_TLS_VOLUME,
                    KAFKASQL_TLS_MOUNT,
                    KAFKASQL_PKCS12_VOLUME,
                    KAFKASQL_PKCS12_MOUNT,
                    KAFKASQL_KEYSTORE_PASS));
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
                        .withNewMetadata()
                            .withLabels(labels)
                            .withAnnotations(configHash != null && !configHash.isBlank()
                                    ? Map.of(CONFIG_HASH_ANNOTATION, configHash)
                                    : Map.of())
                        .endMetadata()
                        .withNewSpec()
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private static EnvVar envVar(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "apicurio-registry",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
