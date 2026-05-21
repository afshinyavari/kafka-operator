package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReference;
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
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.infra.PemToPkcs12InitContainer;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds the MM2 worker Deployment: one container running
 * {@code bin/connect-mirror-maker.sh /etc/mm2/mm2.properties}, with init containers
 * that materialise each side's PEM TLS Secret as a PKCS12 keystore on a shared emptyDir.
 *
 * <p>Mounts (see {@link Mm2ConfigBuilder} for the matching paths):
 * <ul>
 *   <li>{@code /etc/mm2/mm2.properties} — from the ConfigMap.
 *   <li>{@code /etc/mm2/pkcs12/{source,target}/{keystore,truststore}.p12} — emptyDir
 *       populated by the per-side init containers.
 *   <li>{@code /etc/mm2/sasl/{source,target}/} — SASL Secret mounts (optional).
 *   <li>{@code /etc/mm2/registry-auth/{source,target}/} — schema-registry auth Secret
 *       mounts (optional).
 * </ul>
 *
 * <p>Replicas spread across MCS clusters via topology spread on the
 * {@code topology.kubernetes.io/zone} key — workers form a single Connect group via
 * the internal topics, and Kafka's group coordinator distributes tasks.
 */
@ApplicationScoped
public class Mm2DeploymentBuilder {

    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";
    private static final String CONFIG_VOLUME = "mm2-config";
    private static final String CONFIG_MOUNT = "/etc/mm2";
    private static final String JAVA_OPTS = "-XX:MaxRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0";

    public Deployment build(MirrorMaker2 cr,
                            ResolvedEndpoint source, ResolvedEndpoint target,
                            int replicas, String configHash, OwnerReference ownerRef) {
        MirrorMaker2Spec spec = cr.getSpec();
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        Map<String, String> labels = Mm2Labels.labels(name);

        List<Volume> volumes = new ArrayList<>();
        volumes.add(new VolumeBuilder()
                .withName(CONFIG_VOLUME)
                .withNewConfigMap().withName(name).endConfigMap()
                .build());

        List<VolumeMount> workerMounts = new ArrayList<>();
        workerMounts.add(new VolumeMountBuilder()
                .withName(CONFIG_VOLUME)
                .withMountPath(CONFIG_MOUNT + "/mm2.properties")
                .withSubPath(Mm2ConfigMapBuilder.PROPERTIES_KEY)
                .withReadOnly(true).build());

        List<Container> initContainers = new ArrayList<>();
        // Per-side TLS PKCS12 conversion. The init containers reuse the operator's shared
        // PemToPkcs12InitContainer helper.
        String pkcs12VolBase = "mm2-pkcs12";
        if (source.hasTls()) {
            String tlsVol = "tls-source";
            String pkcs12Vol = pkcs12VolBase + "-source";
            volumes.add(secretVol(tlsVol, source.tlsSecretRef()));
            volumes.add(emptyDirVol(pkcs12Vol));
            initContainers.add(PemToPkcs12InitContainer.build(
                    "pem-to-pkcs12-source", spec.getImage(),
                    tlsVol, "/etc/mm2/tls/source",
                    pkcs12Vol, Mm2ConfigBuilder.PKCS12_BASE + "/source",
                    Mm2ConfigBuilder.PKCS12_PASSWORD));
            workerMounts.add(new VolumeMountBuilder()
                    .withName(pkcs12Vol)
                    .withMountPath(Mm2ConfigBuilder.PKCS12_BASE + "/source")
                    .withReadOnly(true).build());
        }
        if (target.hasTls()) {
            String tlsVol = "tls-target";
            String pkcs12Vol = pkcs12VolBase + "-target";
            volumes.add(secretVol(tlsVol, target.tlsSecretRef()));
            volumes.add(emptyDirVol(pkcs12Vol));
            initContainers.add(PemToPkcs12InitContainer.build(
                    "pem-to-pkcs12-target", spec.getImage(),
                    tlsVol, "/etc/mm2/tls/target",
                    pkcs12Vol, Mm2ConfigBuilder.PKCS12_BASE + "/target",
                    Mm2ConfigBuilder.PKCS12_PASSWORD));
            workerMounts.add(new VolumeMountBuilder()
                    .withName(pkcs12Vol)
                    .withMountPath(Mm2ConfigBuilder.PKCS12_BASE + "/target")
                    .withReadOnly(true).build());
        }
        if (source.hasSasl()) {
            mountSecret(volumes, workerMounts, "sasl-source",
                    source.sasl().secretRef(), Mm2ConfigBuilder.SASL_BASE + "/source");
        }
        if (target.hasSasl()) {
            mountSecret(volumes, workerMounts, "sasl-target",
                    target.sasl().secretRef(), Mm2ConfigBuilder.SASL_BASE + "/target");
        }
        if (source.schemaRegistryAuthSecretRef() != null) {
            mountSecret(volumes, workerMounts, "reg-auth-source",
                    source.schemaRegistryAuthSecretRef(), Mm2ConfigBuilder.REGISTRY_AUTH_BASE + "/source");
        }
        if (target.schemaRegistryAuthSecretRef() != null) {
            mountSecret(volumes, workerMounts, "reg-auth-target",
                    target.schemaRegistryAuthSecretRef(), Mm2ConfigBuilder.REGISTRY_AUTH_BASE + "/target");
        }

        Container worker = new ContainerBuilder()
                .withName("mm2")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("/opt/kafka/bin/connect-mirror-maker.sh")
                .withArgs(CONFIG_MOUNT + "/mm2.properties")
                .withEnv(new EnvVarBuilder()
                        .withName("KAFKA_HEAP_OPTS")
                        .withValue(JAVA_OPTS)
                        .build())
                .withVolumeMounts(workerMounts)
                .withResources(new ResourceRequirementsBuilder()
                        .withRequests(quantities(spec.getResources().getRequests()))
                        .withLimits(quantities(spec.getResources().getLimits()))
                        .build())
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();

        List<TopologySpreadConstraint> spread = List.of(new TopologySpreadConstraintBuilder()
                .withMaxSkew(1)
                .withTopologyKey("topology.kubernetes.io/zone")
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withNewLabelSelector().withMatchLabels(labels).endLabelSelector()
                .build());

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
                            .withAnnotations(configHash != null && !configHash.isBlank()
                                    ? Map.of(CONFIG_HASH_ANNOTATION, configHash)
                                    : Map.of())
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

    private static Volume secretVol(String name, String secretName) {
        return new VolumeBuilder()
                .withName(name)
                .withNewSecret().withSecretName(secretName).endSecret()
                .build();
    }

    private static Volume emptyDirVol(String name) {
        return new VolumeBuilder()
                .withName(name)
                .withNewEmptyDir().endEmptyDir()
                .build();
    }

    private static void mountSecret(List<Volume> volumes, List<VolumeMount> mounts,
                                     String volName, String secretName, String mountPath) {
        volumes.add(secretVol(volName, secretName));
        mounts.add(new VolumeMountBuilder()
                .withName(volName)
                .withMountPath(mountPath)
                .withReadOnly(true).build());
    }

    private static Map<String, Quantity> quantities(Map<String, String> in) {
        if (in == null) return Map.of();
        Map<String, Quantity> out = new java.util.LinkedHashMap<>();
        in.forEach((k, v) -> out.put(k, Quantity.parse(v)));
        return out;
    }

    static IntOrString port(int p) { return new IntOrString(p); }
}
