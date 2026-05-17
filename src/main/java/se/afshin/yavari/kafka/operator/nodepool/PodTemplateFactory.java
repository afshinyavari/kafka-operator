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
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
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

            List<Volume> volumes = buildVolumes(poolName, podName);
            List<EnvVar> env = buildEnv(kafkaClusterId, kafkaVersion, configHash, isController, isBroker, zone);
            List<VolumeMount> mounts = buildMounts();
            List<ContainerPort> ports = buildContainerPorts(isController, isBroker);

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
                                .withPort(new IntOrString(isBroker ? BROKER_PORT : CONTROLLER_PORT))
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

            if (hasRack && !zone.isEmpty()) {
                podSpecBuilder.withAffinity(buildZoneAffinity(rackTopologyKey, zone));
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

    private Affinity buildZoneAffinity(String rackTopologyKey, String zone) {
        return new AffinityBuilder()
                .withNewNodeAffinity()
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
                .endNodeAffinity()
                .build();
    }

    private List<Volume> buildVolumes(String poolName, String podName) {
        return List.of(
            new VolumeBuilder()
                    .withName("config")
                    .withNewConfigMap()
                        .withName(poolName + "-config")
                        .withDefaultMode(0755)
                    .endConfigMap()
                    .build(),
            new VolumeBuilder()
                    .withName("data")
                    .withNewPersistentVolumeClaim()
                        .withClaimName("data-" + podName)
                    .endPersistentVolumeClaim()
                    .build()
        );
    }

    private List<EnvVar> buildEnv(String kafkaClusterId, String kafkaVersion, String configHash,
                                   boolean isController, boolean isBroker, String zone) {
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

    private List<VolumeMount> buildMounts() {
        return List.of(
            new VolumeMountBuilder().withName("config").withMountPath("/opt/kafka-config").build(),
            new VolumeMountBuilder().withName("data").withMountPath("/var/lib/kafka/data").build()
        );
    }

    private List<ContainerPort> buildContainerPorts(boolean isController, boolean isBroker) {
        List<ContainerPort> ports = new ArrayList<>();
        if (isBroker) {
            ports.add(new ContainerPortBuilder().withName("kafka").withContainerPort(BROKER_PORT).build());
        }
        if (isController) {
            ports.add(new ContainerPortBuilder().withName("controller").withContainerPort(CONTROLLER_PORT).build());
        }
        return ports;
    }
}
