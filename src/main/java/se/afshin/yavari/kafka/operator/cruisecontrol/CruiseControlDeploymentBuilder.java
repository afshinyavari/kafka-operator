package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.CruiseControlApiSecurity;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;
import se.afshin.yavari.kafka.operator.infra.PemToPkcs12InitContainer;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the Cruise Control {@link Deployment} — a single replica (Cruise Control is a
 * singleton). Modelled on {@code ApicurioDeploymentBuilder}: a config-hash annotation on
 * the pod template drives rolling restarts, and a {@link PemToPkcs12InitContainer} converts
 * the cert-manager PEM Secret into the PKCS12 keystore Cruise Control's Kafka client needs.
 */
@ApplicationScoped
public class CruiseControlDeploymentBuilder {

    /** PodTemplate annotation carrying the operator's config hash. Flipping it rolls the pod. */
    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";

    /** Port the bundled jmx_prometheus_javaagent serves Cruise Control metrics on. Only
     *  exposed as a container port when metrics are enabled. Matches the broker convention. */
    public static final int METRICS_PORT = 9101;

    private static final String CONFIG_VOLUME = "cc-config";
    private static final String CLIENT_TLS_VOLUME = "cc-client-tls";
    private static final String PKCS12_VOLUME = "cc-pkcs12";
    private static final String AUTH_VOLUME = "cc-auth";
    /** Where the input PEM Secret is mounted for the init container (outside CONFIG_DIR). */
    private static final String CLIENT_PEM_DIR = "/etc/cc-client-tls";

    /**
     * @param spec               Cruise Control sub-spec
     * @param namespace          target namespace
     * @param configHash         hash over rendered config + mounted Secret revisions
     * @param mtls               true when the broker INTERNAL listener is mTLS
     * @param ccClientCertSecret cert-manager PEM Secret name (only used when {@code mtls})
     * @param initImage          image carrying openssl + keytool (the Kafka image)
     * @param metricsEnabled     true when KafkaCluster.spec.metricsConfig is set — attaches
     *                           the JMX exporter agent and exposes the metrics port
     */
    public Deployment build(KafkaClusterCruiseControlSpec spec, String namespace,
                            String configHash, boolean mtls, String ccClientCertSecret,
                            String initImage, boolean metricsEnabled) {
        Map<String, String> labels = labels();
        CruiseControlApiSecurity api = spec.getApiSecurity();
        boolean authEnabled = api != null && api.isEnabled() && api.getBasicAuthSecretRef() != null;

        List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMountBuilder()
                .withName(CONFIG_VOLUME)
                .withMountPath(CruiseControlConfigBuilder.CONFIG_DIR)
                .withReadOnly(true)
                .build());

        List<Volume> volumes = new ArrayList<>();
        volumes.add(new VolumeBuilder()
                .withName(CONFIG_VOLUME)
                .withNewConfigMap().withName(CruiseControlOrchestrator.CONFIG_MAP_NAME).endConfigMap()
                .build());

        List<Container> initContainers = new ArrayList<>();
        if (mtls) {
            mounts.add(new VolumeMountBuilder()
                    .withName(PKCS12_VOLUME)
                    .withMountPath(CruiseControlConfigBuilder.TLS_DIR)
                    .withReadOnly(true)
                    .build());
            volumes.add(new VolumeBuilder()
                    .withName(CLIENT_TLS_VOLUME)
                    .withNewSecret().withSecretName(ccClientCertSecret).endSecret()
                    .build());
            volumes.add(new VolumeBuilder()
                    .withName(PKCS12_VOLUME)
                    .withNewEmptyDir().endEmptyDir()
                    .build());
            initContainers.add(PemToPkcs12InitContainer.build(
                    "pem-to-pkcs12", initImage,
                    CLIENT_TLS_VOLUME, CLIENT_PEM_DIR,
                    PKCS12_VOLUME, CruiseControlConfigBuilder.TLS_DIR,
                    CruiseControlConfigBuilder.KEYSTORE_PASSWORD));
        }
        if (authEnabled) {
            mounts.add(new VolumeMountBuilder()
                    .withName(AUTH_VOLUME)
                    .withMountPath(CruiseControlConfigBuilder.AUTH_DIR)
                    .withReadOnly(true)
                    .build());
            volumes.add(new VolumeBuilder()
                    .withName(AUTH_VOLUME)
                    .withNewSecret().withSecretName(api.getBasicAuthSecretRef()).endSecret()
                    .build());
        }

        List<EnvVar> env = new ArrayList<>();
        env.add(new EnvVarBuilder()
                .withName("KAFKA_HEAP_OPTS")
                .withValue("-XX:MaxRAMPercentage=70.0")
                .build());
        if (metricsEnabled) {
            // The Cruise Control start script honors KAFKA_OPTS — attach the JMX exporter
            // agent reading the jmx-config.yaml key the operator added to the config ConfigMap.
            env.add(new EnvVarBuilder()
                    .withName("KAFKA_OPTS")
                    .withValue("-javaagent:/opt/jmx-exporter/jmx-exporter.jar=" + METRICS_PORT
                            + ":" + CruiseControlConfigBuilder.CONFIG_DIR + "/jmx-config.yaml")
                    .build());
        }

        ContainerBuilder containerBuilder = new ContainerBuilder()
                .withName(CruiseControlOrchestrator.CC_NAME)
                .withImage(spec.getImage())
                // The image ENTRYPOINT is the Cruise Control start script; pass the
                // mounted properties file as its argument.
                .withArgs(CruiseControlConfigBuilder.CONFIG_DIR + "/cruisecontrol.properties")
                .addNewPort()
                    .withName("cc-rest")
                    .withContainerPort(CruiseControlOrchestrator.REST_PORT)
                .endPort();
        if (metricsEnabled) {
            containerBuilder.addNewPort()
                    .withName("metrics")
                    .withContainerPort(METRICS_PORT)
                    .endPort();
        }
        Container container = containerBuilder
                .withEnv(env)
                .withVolumeMounts(mounts)
                .withResources(resources(spec.getResources()))
                .withNewReadinessProbe()
                    .withNewHttpGet()
                        .withPath("/kafkacruisecontrol/state")
                        .withNewPort(CruiseControlOrchestrator.REST_PORT)
                    .endHttpGet()
                    .withInitialDelaySeconds(30)
                    .withPeriodSeconds(15)
                    .withFailureThreshold(6)
                .endReadinessProbe()
                .withNewLivenessProbe()
                    // Cruise Control's REST listener responds to any path (404 on unknown), so
                    // a bare TCP probe is enough to detect a hung JVM. /state would require
                    // the analyzer to be warmed up first — that's a readiness concern, not
                    // liveness.
                    .withNewTcpSocket()
                        .withPort(new io.fabric8.kubernetes.api.model.IntOrString(
                                CruiseControlOrchestrator.REST_PORT))
                    .endTcpSocket()
                    .withInitialDelaySeconds(120)
                    .withPeriodSeconds(30)
                    .withFailureThreshold(3)
                .endLivenessProbe()
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(CruiseControlOrchestrator.CC_NAME)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    // Singleton — exactly one Cruise Control instance per Kafka cluster.
                    .withReplicas(1)
                    .withNewSelector().withMatchLabels(labels).endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labels)
                            .withAnnotations(configHash != null && !configHash.isBlank()
                                    ? Map.of(CONFIG_HASH_ANNOTATION, configHash)
                                    : Map.of())
                        .endMetadata()
                        .withNewSpec()
                            .withInitContainers(initContainers)
                            .withContainers(container)
                            .withVolumes(volumes)
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private static ResourceRequirements resources(ResourceRequirements override) {
        if (override != null
                && ((override.getRequests() != null && !override.getRequests().isEmpty())
                 || (override.getLimits() != null && !override.getLimits().isEmpty()))) {
            return override;
        }
        return new ResourceRequirementsBuilder()
                .withRequests(Map.of(
                        "cpu", Quantity.parse("500m"),
                        "memory", Quantity.parse("1Gi")))
                .withLimits(Map.of(
                        "cpu", Quantity.parse("1"),
                        "memory", Quantity.parse("2Gi")))
                .build();
    }

    static Map<String, String> labels() {
        return Map.of(
                "app", CruiseControlOrchestrator.CC_NAME,
                "app.managed-by", "kafka-operator");
    }
}
