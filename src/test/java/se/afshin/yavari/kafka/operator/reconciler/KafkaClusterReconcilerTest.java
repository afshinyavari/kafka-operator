package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.cluster.ClusterStatusAggregator;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaClusterReconcilerTest {

    private static final String NS = "kafka";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String LOCAL_CLUSTER_ID = "A";

    // Common dependencies
    private KubernetesClient client;
    private KRaftConfigGenerator kraftConfig;
    private ClusterStatusAggregator statusAggregator;
    private Context<KafkaCluster> context;
    private KafkaClusterReconciler reconciler;

    // ConfigMaps chain
    private MixedOperation cmOp;
    private NonNamespaceOperation nsCmOp;
    private Resource cmResourceOp;

    // KafkaPodSet list chain (for statusAggregator input)
    private MixedOperation podSetMixedOp;
    private NonNamespaceOperation nsPodSetOp;
    private NonNamespaceOperation labeledPodSetOp;

    // KafkaNodePool list + delete chain (for cleanup)
    private MixedOperation npMixedOp;
    private NonNamespaceOperation nsNpOp;
    private NonNamespaceOperation labeledNpOp;
    private Resource brokerPoolResource;
    private Resource ctrlPoolResource;

    // Pod list chain (for cleanup broker-pod check)
    private MixedOperation podsOp;
    private NonNamespaceOperation nsPodsOp;
    private NonNamespaceOperation labeledPodsOp;

    @BeforeEach
    void setup() throws Exception {
        kraftConfig = mock(KRaftConfigGenerator.class);
        statusAggregator = mock(ClusterStatusAggregator.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        // configMaps chain
        cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        cmResourceOp = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.resource(any(ConfigMap.class))).thenReturn(cmResourceOp);

        // KafkaPodSet list chain
        podSetMixedOp = mock(MixedOperation.class);
        nsPodSetOp = mock(NonNamespaceOperation.class);
        labeledPodSetOp = mock(NonNamespaceOperation.class);
        when(client.resources(KafkaPodSet.class)).thenReturn(podSetMixedOp);
        when(podSetMixedOp.inNamespace(NS)).thenReturn(nsPodSetOp);
        when(nsPodSetOp.withLabel(anyString(), anyString())).thenReturn(labeledPodSetOp);
        stubPodSetList(List.of());

        // KafkaNodePool chain (cleanup)
        npMixedOp = mock(MixedOperation.class);
        nsNpOp = mock(NonNamespaceOperation.class);
        labeledNpOp = mock(NonNamespaceOperation.class);
        brokerPoolResource = mock(Resource.class);
        ctrlPoolResource = mock(Resource.class);
        when(client.resources(KafkaNodePool.class)).thenReturn(npMixedOp);
        when(npMixedOp.inNamespace(NS)).thenReturn(nsNpOp);
        when(nsNpOp.withLabel(anyString(), anyString())).thenReturn(labeledNpOp);
        when(nsNpOp.withName("broker-pool")).thenReturn(brokerPoolResource);
        when(nsNpOp.withName("ctrl-pool")).thenReturn(ctrlPoolResource);

        // Pods chain (cleanup broker-pod check)
        podsOp = mock(MixedOperation.class);
        nsPodsOp = mock(NonNamespaceOperation.class);
        labeledPodsOp = mock(NonNamespaceOperation.class);
        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace(NS)).thenReturn(nsPodsOp);
        when(nsPodsOp.withLabel(anyString(), anyString())).thenReturn(labeledPodsOp);
        stubBrokerPods(List.of());

        // KRaftConfigGenerator stubs
        when(kraftConfig.buildQuorumVoters(any())).thenReturn("10000@ctrl-a:9093");
        when(kraftConfig.clusterIdFrom(any())).thenReturn("cluster-id-abc");

        reconciler = new KafkaClusterReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "kraftConfig", kraftConfig);
        injectField(reconciler, "statusAggregator", statusAggregator);
        injectField(reconciler, "localClusterId", LOCAL_CLUSTER_ID);
    }

    @Test
    void validation_emptyClusterList_setsFailedPhase() {
        KafkaCluster cr = clusterCr(List.of());

        reconciler.reconcile(cr, context);

        assertThat(cr.getStatus().getPhase()).isEqualTo(KafkaClusterStatus.Phase.FAILED);
        verify(cmResourceOp, never()).serverSideApply();
    }

    @Test
    void reconcile_happyPath_appliesQuorumConfigMapAndCallsAggregator() {
        KafkaCluster cr = validClusterCr();
        doAnswer(inv -> {
            KafkaClusterStatus s = inv.getArgument(0);
            s.setPhase(KafkaClusterStatus.Phase.READY);
            return null;
        }).when(statusAggregator).aggregate(any(), any());

        reconciler.reconcile(cr, context);

        verify(cmResourceOp).serverSideApply();
        verify(statusAggregator).aggregate(any(), any());
        assertThat(cr.getStatus().getPhase()).isEqualTo(KafkaClusterStatus.Phase.READY);
    }

    @Test
    void cleanup_brokerPodsGone_deletesAllPools() {
        KafkaCluster cr = validClusterCr();
        stubPoolList(List.of(
                nodePool("broker-pool", NodeRole.BROKER),
                nodePool("ctrl-pool", NodeRole.CONTROLLER)
        ));
        stubBrokerPods(List.of()); // no broker pods remaining

        reconciler.cleanup(cr, context);

        verify(brokerPoolResource).delete();
        verify(ctrlPoolResource).delete();
    }

    @Test
    void cleanup_brokerPodsStillRunning_defersControllerDeletion() {
        KafkaCluster cr = validClusterCr();
        stubPoolList(List.of(
                nodePool("broker-pool", NodeRole.BROKER),
                nodePool("ctrl-pool", NodeRole.CONTROLLER)
        ));
        stubBrokerPods(List.of(podWithNodeId(0))); // broker pod (nodeId < CONTROLLER_BASE) still running

        reconciler.cleanup(cr, context);

        verify(brokerPoolResource).delete();
        verify(ctrlPoolResource, never()).delete(); // deferred until broker pods are gone
    }

    // --- Helpers ---

    private void stubPodSetList(List<KafkaPodSet> sets) {
        var list = mock(io.fabric8.kubernetes.api.model.KubernetesResourceList.class);
        when(list.getItems()).thenReturn((List) sets);
        when(labeledPodSetOp.list()).thenReturn(list);
    }

    private void stubPoolList(List<KafkaNodePool> pools) {
        var list = mock(io.fabric8.kubernetes.api.model.KubernetesResourceList.class);
        when(list.getItems()).thenReturn((List) pools);
        when(labeledNpOp.list()).thenReturn(list);
    }

    private void stubBrokerPods(List<Pod> pods) {
        PodList podList = new PodList();
        podList.setItems(pods);
        when(labeledPodsOp.list()).thenReturn(podList);
    }

    private KafkaCluster validClusterCr() {
        ClusterEntry entry = new ClusterEntry();
        entry.setId(LOCAL_CLUSTER_ID);
        entry.setControllerAdvertisedAddress("ctrl-a.example.com:9093");
        return clusterCr(List.of(entry));
    }

    private KafkaCluster clusterCr(List<ClusterEntry> clusters) {
        KafkaCluster cr = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(CLUSTER_NAME);
        meta.setNamespace(NS);
        meta.setUid("uid-123");
        cr.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(clusters);
        spec.setKafkaImage("apache/kafka:3.7.0");
        cr.setSpec(spec);
        return cr;
    }

    private KafkaNodePool nodePool(String name, NodeRole role) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        // no deletionTimestamp → eligible for deletion
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(List.of(role));
        pool.setSpec(spec);
        return pool;
    }

    private Pod podWithNodeId(int nodeId) {
        Pod pod = new Pod();
        ObjectMeta meta = new ObjectMeta();
        meta.setLabels(Map.of(KafkaPodSet.NODE_ID_LABEL, String.valueOf(nodeId)));
        pod.setMetadata(meta);
        return pod;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
