package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
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

@ApplicationScoped
public class PodTemplateFactory {

    private static final int BROKER_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;

    @Inject
    KRaftConfigGenerator kraftConfig;

    public List<PodEntry> build(KafkaNodePool pool, KafkaCluster cluster, String namespace,
                                int clusterIndex, String kafkaClusterId, String configHash,
                                boolean isBroker, boolean isController) {
        List<PodEntry> pods = new ArrayList<>();
        String poolName = pool.getMetadata().getName();
        String clusterName = cluster.getMetadata().getName();
        String kafkaImage = cluster.getSpec().getKafkaImage();

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
                    .build();

            String rackTopologyKey = pool.getSpec().getRackTopologyKey();
            boolean hasRack = isBroker && rackTopologyKey != null && !rackTopologyKey.isBlank();

            List<Volume> volumes = buildVolumes(poolName, podName, hasRack);
            List<EnvVar> env = buildEnv(kafkaClusterId, configHash, isController, isBroker);
            List<VolumeMount> mounts = buildMounts(hasRack);
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

            List<Container> initContainers = new ArrayList<>();
            if (hasRack) {
                String labelKey = rackTopologyKey.replace(".", "\\.").replace("/", "\\/");
                String initScript = "kubectl get node \"$NODE_NAME\" "
                        + "-o jsonpath=\"{.metadata.labels['" + labelKey + "']}\" "
                        + "> /opt/kafka/init/rack.id";
                initContainers.add(new ContainerBuilder()
                        .withName("rack-init")
                        .withImage("bitnami/kubectl:latest")
                        .withCommand("/bin/sh", "-c", initScript)
                        .withEnv(new EnvVarBuilder()
                                .withName("NODE_NAME")
                                .withNewValueFrom()
                                    .withNewFieldRef().withFieldPath("spec.nodeName").endFieldRef()
                                .endValueFrom()
                                .build())
                        .withVolumeMounts(new VolumeMountBuilder()
                                .withName("rack-init").withMountPath("/opt/kafka/init").build())
                        .build());
            }

            PodSpecBuilder podSpecBuilder = new PodSpecBuilder()
                    .withContainers(container)
                    .withVolumes(volumes)
                    .withRestartPolicy("Always")
                    .withHostname(podName)
                    .withSubdomain(poolName + "-headless");
            if (hasRack) {
                podSpecBuilder
                        .withInitContainers(initContainers)
                        .withServiceAccountName("kafka-node");
            }

            PodEntry entry = new PodEntry();
            entry.setMetadata(meta);
            entry.setSpec(podSpecBuilder.build());
            pods.add(entry);
        }
        return pods;
    }

    private List<Volume> buildVolumes(String poolName, String podName, boolean hasRack) {
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
        if (hasRack) {
            volumes.add(new VolumeBuilder()
                    .withName("rack-init")
                    .withNewEmptyDir().endEmptyDir()
                    .build());
        }
        return volumes;
    }

    private List<EnvVar> buildEnv(String kafkaClusterId, String configHash,
                                   boolean isController, boolean isBroker) {
        List<EnvVar> env = new ArrayList<>();
        env.add(new EnvVarBuilder().withName("KAFKA_HEAP_OPTS")
                .withValue(isController ? "-Xmx512m -Xms512m" : "-Xmx1g -Xms1g").build());
        env.add(new EnvVarBuilder().withName("KAFKA_CLUSTER_ID").withValue(kafkaClusterId).build());
        env.add(new EnvVarBuilder().withName("KAFKA_CONFIG_HASH").withValue(configHash).build());
        if (isBroker) {
            env.add(new EnvVarBuilder()
                    .withName("POD_NAME")
                    .withNewValueFrom()
                        .withNewFieldRef().withFieldPath("metadata.name").endFieldRef()
                    .endValueFrom()
                    .build());
        }
        return env;
    }

    private List<VolumeMount> buildMounts(boolean hasRack) {
        List<VolumeMount> mounts = new ArrayList<>();
        mounts.add(new VolumeMountBuilder().withName("config").withMountPath("/opt/kafka-config").build());
        mounts.add(new VolumeMountBuilder().withName("data").withMountPath("/var/lib/kafka/data").build());
        if (hasRack) {
            mounts.add(new VolumeMountBuilder().withName("rack-init").withMountPath("/opt/kafka/init").build());
        }
        return mounts;
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
