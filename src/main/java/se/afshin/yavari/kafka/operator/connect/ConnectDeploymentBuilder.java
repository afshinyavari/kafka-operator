package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetAction;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaUIProbeConfig;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;
import se.afshin.yavari.kafka.operator.infra.PemToPkcs12InitContainer;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the Connect worker Deployment: one container running
 * {@code bin/connect-distributed.sh /etc/connect/connect-distributed.properties}, with
 * an init container that materialises the attached cluster's PEM TLS Secret into a
 * PKCS12 keystore + truststore on a shared emptyDir.
 *
 * <p>Plugin volumes (PVC + ConfigMaps + Secrets) come from {@link ConnectPluginResolver}
 * and are mounted under {@code /opt/kafka/connect-plugins/...}. The composed
 * {@code plugin.path} is already in the properties file.
 *
 * <p>Replicas spread across MCS clusters via topology spread on the
 * {@code topology.kubernetes.io/zone} key — workers form a single Connect group via
 * the internal topics, and Kafka's group coordinator distributes tasks.
 */
@ApplicationScoped
public class ConnectDeploymentBuilder {

    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";
    private static final String CONFIG_VOLUME = "connect-config";
    private static final String CONFIG_MOUNT = "/etc/connect";
    private static final String JAVA_OPTS = "-XX:MaxRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0";

    /** JMX exporter port — matches the broker / MM2 convention. */
    public static final int METRICS_PORT = 9101;
    private static final String JMX_CONFIG_MOUNT = CONFIG_MOUNT + "/jmx-config.yaml";

    public Deployment build(KafkaConnect cr,
                            ResolvedKafkaEndpoint endpoint,
                            ResolvedPluginSources plugins,
                            int replicas, String configHash, boolean metricsEnabled,
                            OwnerReference ownerRef) {
        KafkaConnectSpec spec = cr.getSpec();
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        Map<String, String> labels = ConnectLabels.labels(name);

        List<Volume> volumes = new ArrayList<>();
        volumes.add(new VolumeBuilder()
                .withName(CONFIG_VOLUME)
                .withNewConfigMap().withName(name).endConfigMap()
                .build());

        List<VolumeMount> workerMounts = new ArrayList<>();
        workerMounts.add(new VolumeMountBuilder()
                .withName(CONFIG_VOLUME)
                .withMountPath(CONFIG_MOUNT + "/" + ConnectConfigMapBuilder.PROPERTIES_KEY)
                .withSubPath(ConnectConfigMapBuilder.PROPERTIES_KEY)
                .withReadOnly(true).build());

        List<Container> initContainers = new ArrayList<>();
        if (endpoint.hasTls()) {
            String tlsVol = "tls";
            String pkcs12Vol = "connect-pkcs12";
            volumes.add(new VolumeBuilder()
                    .withName(tlsVol)
                    .withNewSecret().withSecretName(endpoint.tlsSecretRef()).endSecret()
                    .build());
            volumes.add(new VolumeBuilder()
                    .withName(pkcs12Vol)
                    .withNewEmptyDir().endEmptyDir()
                    .build());
            initContainers.add(PemToPkcs12InitContainer.build(
                    "pem-to-pkcs12", spec.getImage(),
                    tlsVol, "/etc/connect/tls",
                    pkcs12Vol, ConnectConfigBuilder.PKCS12_BASE,
                    ConnectConfigBuilder.PKCS12_PASSWORD));
            workerMounts.add(new VolumeMountBuilder()
                    .withName(pkcs12Vol)
                    .withMountPath(ConnectConfigBuilder.PKCS12_BASE)
                    .withReadOnly(true).build());
        }
        if (endpoint.hasSasl()) {
            String secretName = endpoint.sasl().secretRef();
            volumes.add(new VolumeBuilder()
                    .withName("sasl")
                    .withNewSecret().withSecretName(secretName).endSecret()
                    .build());
            workerMounts.add(new VolumeMountBuilder()
                    .withName("sasl")
                    .withMountPath(ConnectConfigBuilder.SASL_BASE)
                    .withReadOnly(true).build());
        }

        // Plugin volumes + mounts come pre-built from the resolver.
        volumes.addAll(plugins.volumes());
        workerMounts.addAll(plugins.volumeMounts());

        List<EnvVar> env = new ArrayList<>();
        env.add(new EnvVarBuilder()
                .withName("KAFKA_HEAP_OPTS").withValue(JAVA_OPTS).build());
        if (metricsEnabled) {
            workerMounts.add(new VolumeMountBuilder()
                    .withName(CONFIG_VOLUME)
                    .withMountPath(JMX_CONFIG_MOUNT)
                    .withSubPath(ConnectConfigMapBuilder.JMX_CONFIG_KEY)
                    .withReadOnly(true).build());
            env.add(new EnvVarBuilder()
                    .withName("KAFKA_OPTS")
                    .withValue("-javaagent:/opt/jmx-exporter/jmx-exporter.jar=" + METRICS_PORT
                            + ":" + JMX_CONFIG_MOUNT)
                    .build());
        }

        ContainerBuilder workerBuilder = new ContainerBuilder()
                .withName("connect")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("/opt/kafka/bin/connect-distributed.sh")
                .withArgs(CONFIG_MOUNT + "/" + ConnectConfigMapBuilder.PROPERTIES_KEY)
                .withEnv(env)
                .withVolumeMounts(workerMounts)
                .withResources(new ResourceRequirementsBuilder()
                        .withRequests(quantities(spec.getResources().getRequests()))
                        .withLimits(quantities(spec.getResources().getLimits()))
                        .build())
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .addNewPort()
                    .withName(ConnectRestServiceBuilder.PORT_NAME)
                    .withContainerPort(spec.getRestPort())
                .endPort()
                .withReadinessProbe(httpProbe(spec.getProbes().getReadiness(), spec.getRestPort()))
                .withLivenessProbe(httpProbe(spec.getProbes().getLiveness(), spec.getRestPort()));
        if (metricsEnabled) {
            workerBuilder.addNewPort()
                    .withName("metrics")
                    .withContainerPort(METRICS_PORT)
                    .endPort();
        }
        Container worker = workerBuilder.build();

        List<TopologySpreadConstraint> spread = List.of(new TopologySpreadConstraintBuilder()
                .withMaxSkew(1)
                .withTopologyKey("topology.kubernetes.io/zone")
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withNewLabelSelector().withMatchLabels(labels).endLabelSelector()
                .build());

        Map<String, String> annotations = new LinkedHashMap<>();
        if (configHash != null && !configHash.isBlank()) {
            annotations.put(CONFIG_HASH_ANNOTATION, configHash);
        }

        return new DeploymentBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .withNewSelector()
                        .withMatchLabels(labels)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labels)
                            .withAnnotations(annotations)
                        .endMetadata()
                        .withNewSpec()
                            .withInitContainers(initContainers)
                            .withContainers(worker)
                            .withVolumes(volumes)
                            .withTopologySpreadConstraints(spread)
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private static Probe httpProbe(KafkaUIProbeConfig cfg, int port) {
        HTTPGetAction action = new HTTPGetAction();
        action.setPath(cfg.getPath());
        action.setPort(new IntOrString(port));
        return new ProbeBuilder()
                .withHttpGet(action)
                .withInitialDelaySeconds(cfg.getInitialDelaySeconds())
                .withPeriodSeconds(cfg.getPeriodSeconds())
                .build();
    }

    private static Map<String, Quantity> quantities(Map<String, String> in) {
        if (in == null) return Map.of();
        Map<String, Quantity> out = new LinkedHashMap<>();
        in.forEach((k, v) -> out.put(k, Quantity.parse(v)));
        return out;
    }
}
