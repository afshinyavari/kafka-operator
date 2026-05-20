package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class PodTemplateFactoryTest {

    private static final String NS = "kafka";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String POOL_NAME = "brokers-a";
    private static final String IMAGE = "kafka-ubi:4.0.0";
    private static final String RACK_KEY = "topology.kubernetes.io/zone";

    private PodTemplateFactory factory;
    private KubernetesClient client;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        factory = new PodTemplateFactory();
        setField(factory, "kraftConfig", new KRaftConfigGenerator());
        setField(factory, "client", client);
    }

    @Test
    void broker_hasCorrectPortAndLabels() {
        List<PodEntry> pods = factory.build(pool(1, NodeRole.BROKER), cluster(), NS, 0, "cid", "hash", true, false, null);

        assertThat(pods).hasSize(1);
        PodEntry pod = pods.get(0);
        assertThat(pod.getMetadata().getName()).isEqualTo(POOL_NAME + "-0");
        assertThat(pod.getMetadata().getLabels()).containsKey("kafka.yavari.afshin.se/cluster");

        var ports = pod.getSpec().getContainers().get(0).getPorts();
        assertThat(ports).anyMatch(p -> p.getContainerPort() == 9092 && "kafka".equals(p.getName()));
        assertThat(ports).noneMatch(p -> p.getContainerPort() == 9093);
    }

    @Test
    void controller_hasControllerPortAndCgroupSizedHeap() {
        List<PodEntry> pods = factory.build(pool(1, NodeRole.CONTROLLER), cluster(), NS, 0, "cid", "hash", false, true, null);

        assertThat(pods).hasSize(1);
        var container = pods.get(0).getSpec().getContainers().get(0);

        var ports = container.getPorts();
        assertThat(ports).anyMatch(p -> p.getContainerPort() == 9093 && "controller".equals(p.getName()));
        assertThat(ports).noneMatch(p -> p.getContainerPort() == 9092);

        var heapEnv = container.getEnv().stream()
                .filter(e -> "KAFKA_HEAP_OPTS".equals(e.getName()))
                .findFirst().orElseThrow();
        assertThat(heapEnv.getValue())
                .contains("MaxRAMPercentage")
                .contains("InitialRAMPercentage");
    }

    @Test
    void combined_hasBothPorts() {
        List<PodEntry> pods = factory.build(pool(1, NodeRole.BROKER, NodeRole.CONTROLLER), cluster(), NS, 0, "cid", "hash", true, true, null);

        var ports = pods.get(0).getSpec().getContainers().get(0).getPorts();
        assertThat(ports).anyMatch(p -> p.getContainerPort() == 9092);
        assertThat(ports).anyMatch(p -> p.getContainerPort() == 9093);
    }

    @Test
    void brokerWithRack_setsBrokerRackEnvAndAffinity() {
        stubNodes("zone-a", "zone-b");
        KafkaNodePool rackPool = pool(1, NodeRole.BROKER);
        rackPool.getSpec().setRackTopologyKey(RACK_KEY);

        List<PodEntry> pods = factory.build(rackPool, cluster(), NS, 0, "cid", "hash", true, false, null);

        var env = pods.get(0).getSpec().getContainers().get(0).getEnv();
        assertThat(env).anyMatch(e -> "BROKER_RACK".equals(e.getName()) && "zone-a".equals(e.getValue()));
        assertThat(pods.get(0).getSpec().getAffinity()).isNotNull();
    }

    @Test
    void brokerWithoutRack_noBrokerRackEnvOrAffinity() {
        // no rackTopologyKey set on pool
        List<PodEntry> pods = factory.build(pool(1, NodeRole.BROKER), cluster(), NS, 0, "cid", "hash", true, false, null);

        var env = pods.get(0).getSpec().getContainers().get(0).getEnv();
        assertThat(env).noneMatch(e -> "BROKER_RACK".equals(e.getName()));
        // Anti-affinity is always applied; only rack-specific node affinity is absent
        assertThat(pods.get(0).getSpec().getAffinity().getNodeAffinity()).isNull();
    }

    @Test
    void replicaCount_createsCorrectNumberOfPods() {
        List<PodEntry> pods = factory.build(pool(3, NodeRole.BROKER), cluster(), NS, 0, "cid", "hash", true, false, null);

        assertThat(pods).hasSize(3);
        assertThat(pods.get(0).getMetadata().getName()).isEqualTo(POOL_NAME + "-0");
        assertThat(pods.get(1).getMetadata().getName()).isEqualTo(POOL_NAME + "-1");
        assertThat(pods.get(2).getMetadata().getName()).isEqualTo(POOL_NAME + "-2");
    }

    // --- Helpers ---

    private void stubNodes(String... zones) {
        NonNamespaceOperation nodesOp = mock(NonNamespaceOperation.class);
        when(client.nodes()).thenReturn(nodesOp);
        when(nodesOp.withLabel(anyString())).thenReturn(nodesOp);

        NodeList nodeList = new NodeList();
        nodeList.setItems(List.of(
                java.util.Arrays.stream(zones)
                        .map(z -> {
                            Node n = new Node();
                            n.setMetadata(new ObjectMeta());
                            n.getMetadata().setLabels(Map.of(RACK_KEY, z));
                            return n;
                        })
                        .toArray(Node[]::new)
        ));
        when(nodesOp.list()).thenReturn(nodeList);
    }

    private KafkaNodePool pool(int replicas, NodeRole... roles) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setReplicas(replicas);
        spec.setRoles(List.of(roles));
        pool.setSpec(spec);
        return pool;
    }

    private KafkaCluster cluster() {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(CLUSTER_NAME);
        meta.setUid("cluster-uid");
        c.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setKafkaImage(IMAGE);
        c.setSpec(spec);
        return c;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
