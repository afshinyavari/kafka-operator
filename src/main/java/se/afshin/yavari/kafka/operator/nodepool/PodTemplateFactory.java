package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PreferredSchedulingTermBuilder;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraint;
import io.fabric8.kubernetes.api.model.TopologySpreadConstraintBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.WeightedPodAffinityTermBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.MetricsConfig;
import se.afshin.yavari.kafka.operator.crd.PodEntry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class PodTemplateFactory {

    private static final int BROKER_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;

    @Inject KRaftConfigGenerator kraftConfig;
    @Inject KubernetesClient client;

    public List<PodEntry> build(KafkaNodePool pool, KafkaCluster cluster, String namespace,
                                int clusterIndex, String kafkaClusterId, String configHash,
                                boolean isBroker, boolean isController) {
        List<PodEntry> pods = new ArrayList<>();
        String poolName = pool.getMetadata().getName();
        String clusterName = cluster.getMetadata().getName();
        String kafkaImage = cluster.getSpec().getKafkaImage();
        String kafkaVersion = cluster.getSpec().getKafkaVersion();

        String rackTopologyKey = pool.getSpec().getRackTopologyKey();
        boolean hasRack = isBroker && rackTopologyKey != null && !rackTopologyKey.isBlank();
        MetricsConfig metrics = cluster.getSpec().getMetricsConfig();
        boolean hasMetrics = metrics != null && metrics.getConfigMapRef() != null;
        List<KafkaListenerSpec> listeners = cluster.getSpec().getListeners();
        KafkaListenerTlsConfig controllerTls = cluster.getSpec().getControllerTls();
        boolean needsTls = (controllerTls != null) || (isBroker && hasTlsListeners(listeners));

        // Resolve zones once per pool — sorted for deterministic assignment
        List<String> zones = hasRack ? resolveZones(rackTopologyKey) : List.of();

        for (int i = 0; i < pool.getSpec().getReplicas(); i++) {
            String podName = poolName + "-" + i;
            int nodeId = isController
                    ? kraftConfig.controllerNodeId(clusterIndex)
                    : kraftConfig.brokerNodeId(clusterIndex, i);

            Map<String, String> labels = new LinkedHashMap<>();
            labels.put(KafkaPodSet.CLUSTER_LABEL,    clusterName);
            labels.put(KafkaPodSet.NODE_POOL_LABEL,  poolName);
            labels.put(KafkaPodSet.NODE_ID_LABEL,    String.valueOf(nodeId));
            labels.put(KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE);

            ObjectMeta meta = new ObjectMetaBuilder()
                    .withName(podName)
                    .withNamespace(namespace)
                    .withLabels(labels)
                    .withAnnotations(Map.of(KafkaPodSet.KAFKA_VERSION_ANNOTATION, kafkaVersion))
                    .build();

            String zone = zones.isEmpty() ? "" : zones.get(i % zones.size());

            List<Volume> volumes = buildVolumes(poolName, podName, hasMetrics, metrics, needsTls);
            List<EnvVar> env = buildEnv(kafkaClusterId, kafkaVersion, configHash,
                    isController, isBroker, zone, hasMetrics, listeners, i);
            List<VolumeMount> mounts = buildMounts(hasMetrics, needsTls);
            List<ContainerPort> ports = buildContainerPorts(isController, isBroker, hasMetrics, listeners);

            Container container = new ContainerBuilder()
                    .withName("kafka")
                    .withImage(kafkaImage)
                    .withCommand("/bin/bash", "/opt/kafka-config/start.sh")
                    .withPorts(ports)
                    .withEnv(env)
                    .withVolumeMounts(mounts)
                    .withResources(pool.getSpec().getResources())
                    .withReadinessProbe(new ProbeBuilder()
                            .withNewTcpSocket()
                                .withPort(new IntOrString(brokerReadinessPort(isBroker, listeners)))
                            .endTcpSocket()
                            .withInitialDelaySeconds(isBroker ? 45 : 30)
                            .withPeriodSeconds(15)
                            .withFailureThreshold(6)
                            .build())
                    .build();

            PodSpecBuilder podSpecBuilder = new PodSpecBuilder()
                    .withContainers(container)
                    .withVolumes(volumes)
                    .withRestartPolicy("Always")
                    .withHostname(podName)
                    .withSubdomain(poolName + "-headless");

            podSpecBuilder.withAffinity(buildAffinity(rackTopologyKey, hasRack ? zone : "", poolName, clusterName));
            if (hasRack) {
                podSpecBuilder.withTopologySpreadConstraints(buildTopologySpread(rackTopologyKey, poolName, clusterName));
            }

            PodEntry entry = new PodEntry();
            entry.setMetadata(meta);
            entry.setSpec(podSpecBuilder.build());
            pods.add(entry);
        }
        return pods;
    }

    private List<String> resolveZones(String rackTopologyKey) {
        return client.nodes()
                .withLabel(rackTopologyKey)
                .list().getItems().stream()
                .map(n -> n.getMetadata().getLabels().get(rackTopologyKey))
                .filter(z -> z != null && !z.isBlank())
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    private Affinity buildAffinity(String rackTopologyKey, String zone, String poolName, String clusterName) {
        AffinityBuilder builder = new AffinityBuilder();

        if (rackTopologyKey != null && !zone.isEmpty()) {
            builder.withNewNodeAffinity()
                    .addToPreferredDuringSchedulingIgnoredDuringExecution(
                        new PreferredSchedulingTermBuilder()
                            .withWeight(100)
                            .withPreference(new NodeSelectorTermBuilder()
                                .addToMatchExpressions(new NodeSelectorRequirementBuilder()
                                    .withKey(rackTopologyKey)
                                    .withOperator("In")
                                    .withValues(zone)
                                    .build())
                                .build())
                            .build())
                    .endNodeAffinity();
        }

        // Prefer not to co-schedule pods from the same pool on the same node
        builder.withNewPodAntiAffinity()
                .addToPreferredDuringSchedulingIgnoredDuringExecution(
                    new WeightedPodAffinityTermBuilder()
                        .withWeight(100)
                        .withNewPodAffinityTerm()
                            .withTopologyKey("kubernetes.io/hostname")
                            .withNewLabelSelector()
                                .withMatchLabels(Map.of(
                                    KafkaPodSet.NODE_POOL_LABEL, poolName,
                                    KafkaPodSet.CLUSTER_LABEL,   clusterName
                                ))
                            .endLabelSelector()
                        .endPodAffinityTerm()
                        .build())
                .endPodAntiAffinity();

        return builder.build();
    }

    private TopologySpreadConstraint buildTopologySpread(String topologyKey, String poolName, String clusterName) {
        return new TopologySpreadConstraintBuilder()
                .withMaxSkew(1)
                .withTopologyKey(topologyKey)
                .withWhenUnsatisfiable("ScheduleAnyway")
                .withNewLabelSelector()
                    .withMatchLabels(Map.of(
                        KafkaPodSet.NODE_POOL_LABEL, poolName,
                        KafkaPodSet.CLUSTER_LABEL,   clusterName
                    ))
                .endLabelSelector()
                .build();
    }

    private List<Volume> buildVolumes(String poolName, String podName,
                                      boolean hasMetrics, MetricsConfig metrics, boolean needsTls) {
        List<Volume> volumes = new ArrayList<>();
        volumes.add(new VolumeBuilder()
                .withName("config")
                .withNewConfigMap()
                    .withName(poolName + "-config")
                    .withDefaultMode(0755)
                .endConfigMap()
                .build());
        volumes.add(new VolumeBuilder()
                .withName("data")
                .withNewPersistentVolumeClaim()
                    .withClaimName("data-" + podName)
                .endPersistentVolumeClaim()
                .build());
        if (hasMetrics) {
            volumes.add(new VolumeBuilder()
                    .withName("jmx-config")
                    .withNewConfigMap()
                        .withName(metrics.getConfigMapRef())
                    .endConfigMap()
                    .build());
        }
        if (needsTls) {
            volumes.add(new VolumeBuilder()
                    .withName("tls")
                    .withNewSecret()
                        .withSecretName(podName + "-tls")
                        .withDefaultMode(0440)
                    .endSecret()
                    .build());
        }
        return volumes;
    }

    private List<EnvVar> buildEnv(String kafkaClusterId, String kafkaVersion, String configHash,
                                   boolean isController, boolean isBroker, String zone,
                                   boolean hasMetrics, List<KafkaListenerSpec> listeners, int ordinal) {
        List<EnvVar> env = new ArrayList<>();
        env.add(new EnvVarBuilder().withName("KAFKA_HEAP_OPTS")
                .withValue(isController ? "-Xmx512m -Xms512m" : "-Xmx1g -Xms1g").build());
        env.add(new EnvVarBuilder().withName("KAFKA_CLUSTER_ID").withValue(kafkaClusterId).build());
        env.add(new EnvVarBuilder().withName("KAFKA_VERSION").withValue(kafkaVersion).build());
        env.add(new EnvVarBuilder().withName("KAFKA_CONFIG_HASH").withValue(configHash).build());
        if (isBroker) {
            env.add(new EnvVarBuilder()
                    .withName("POD_NAME")
                    .withNewValueFrom()
                        .withNewFieldRef().withFieldPath("metadata.name").endFieldRef()
                    .endValueFrom()
                    .build());
            if (!zone.isEmpty()) {
                env.add(new EnvVarBuilder().withName("BROKER_RACK").withValue(zone).build());
            }
            boolean hasExternalListeners = listeners != null && listeners.stream()
                    .anyMatch(l -> l.getExternalAccess() != null);
            if (hasExternalListeners) {
                env.add(new EnvVarBuilder()
                        .withName("HOST_IP")
                        .withNewValueFrom()
                            .withNewFieldRef().withFieldPath("status.hostIP").endFieldRef()
                        .endValueFrom()
                        .build());
                for (KafkaListenerSpec l : listeners) {
                    if (l.getExternalAccess() == ExternalAccessType.NODEPORT) {
                        env.add(new EnvVarBuilder()
                                .withName("EXTERNAL_" + l.getName() + "_NODEPORT")
                                .withValue(String.valueOf(l.getNodePortBase() + ordinal))
                                .build());
                    }
                }
            }
        }
        if (isController) {
            env.add(new EnvVarBuilder()
                    .withName("MY_POD_IP")
                    .withNewValueFrom()
                        .withNewFieldRef().withFieldPath("status.podIP").endFieldRef()
                    .endValueFrom()
                    .build());
        }
        return env;
    }

    private List<VolumeMount> buildMounts(boolean hasMetrics, boolean needsTls) {
        List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMountBuilder().withName("config").withMountPath("/opt/kafka-config").build());
        mounts.add(new VolumeMountBuilder().withName("data").withMountPath("/var/lib/kafka/data").build());
        if (hasMetrics) {
            mounts.add(new VolumeMountBuilder().withName("jmx-config").withMountPath("/opt/jmx-exporter-config").build());
        }
        if (needsTls) {
            mounts.add(new VolumeMountBuilder().withName("tls").withMountPath("/etc/kafka/tls").withReadOnly(true).build());
        }
        return mounts;
    }

    private List<ContainerPort> buildContainerPorts(boolean isController, boolean isBroker,
                                                    boolean hasMetrics, List<KafkaListenerSpec> listeners) {
        List<ContainerPort> ports = new ArrayList<>();
        if (isBroker) {
            ports.add(new ContainerPortBuilder().withName("kafka").withContainerPort(BROKER_PORT).build());
        }
        if (isController) {
            ports.add(new ContainerPortBuilder().withName("controller").withContainerPort(CONTROLLER_PORT).build());
        }
        if (hasMetrics) {
            ports.add(new ContainerPortBuilder().withName("jmx").withContainerPort(9101).build());
        }
        if (isBroker && listeners != null) {
            for (KafkaListenerSpec l : listeners) {
                String portName = l.getName().toLowerCase().replace('_', '-');
                ports.add(new ContainerPortBuilder().withName(portName).withContainerPort(l.getPort()).build());
            }
        }
        return ports;
    }

    private boolean hasTlsListeners(List<KafkaListenerSpec> listeners) {
        return listeners != null && listeners.stream().anyMatch(l -> l.getTls() != null);
    }

    private int brokerReadinessPort(boolean isBroker, List<KafkaListenerSpec> listeners) {
        if (!isBroker) return CONTROLLER_PORT;
        // Use the first internal TLS listener for readiness (INTERNAL is localhost-only when TLS is active)
        if (listeners != null) {
            return listeners.stream()
                    .filter(l -> l.getExternalAccess() == null && l.getTls() != null)
                    .findFirst()
                    .map(KafkaListenerSpec::getPort)
                    .orElse(BROKER_PORT);
        }
        return BROKER_PORT;
    }
}
