package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetStatus;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.nodepool.HeadlessServiceBuilder;
import se.afshin.yavari.kafka.operator.nodepool.PodTemplateFactory;
import se.afshin.yavari.kafka.operator.nodepool.PoolConfigMapBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaNodePoolReconcilerTest {

    private static final String NS = "kafka";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String POOL_NAME = "brokers-a";
    private static final String LOCAL_CLUSTER_ID = "A";
    private static final String QUORUM_CM_NAME = CLUSTER_NAME + KafkaClusterReconciler.QUORUM_CONFIG_SUFFIX;
    private static final String POD_SET_NAME = POOL_NAME + "-podset";

    // Dependencies
    private KubernetesClient client;
    private KRaftConfigGenerator kraftConfig;
    private PoolConfigMapBuilder poolConfigMapBuilder;
    private HeadlessServiceBuilder headlessServiceBuilder;
    private PodTemplateFactory podTemplateFactory;
    private Context<KafkaNodePool> context;
    private KafkaNodePoolReconciler reconciler;

    // KafkaCluster lookup chain
    private MixedOperation clusterMixedOp;
    private NonNamespaceOperation nsClusterOp;
    private Resource namedClusterOp;

    // ConfigMaps chain
    private MixedOperation cmOp;
    private NonNamespaceOperation nsCmOp;
    private Resource namedCmOp;    // withName(quorumCmName).get()
    private Resource cmResourceOp; // resource(poolCm).serverSideApply()

    // Services chain
    private MixedOperation svcOp;
    private NonNamespaceOperation nsSvcOp;
    private ServiceResource svcResourceOp;
    private ServiceResource namedSvcOp;

    // KafkaPodSet chain
    private MixedOperation podSetMixedOp;
    private NonNamespaceOperation nsPodSetOp;
    private Resource podSetResourceOp;
    private Resource namedPodSetOp;

    @BeforeEach
    void setup() throws Exception {
        kraftConfig = mock(KRaftConfigGenerator.class);
        poolConfigMapBuilder = mock(PoolConfigMapBuilder.class);
        headlessServiceBuilder = mock(HeadlessServiceBuilder.class);
        podTemplateFactory = mock(PodTemplateFactory.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        // KafkaCluster lookup
        clusterMixedOp = mock(MixedOperation.class);
        nsClusterOp = mock(NonNamespaceOperation.class);
        namedClusterOp = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterMixedOp);
        when(clusterMixedOp.inNamespace(NS)).thenReturn(nsClusterOp);
        when(nsClusterOp.withName(CLUSTER_NAME)).thenReturn(namedClusterOp);
        when(namedClusterOp.get()).thenReturn(validCluster());

        // ConfigMaps: withName for quorum CM, resource() for pool CM serverSideApply
        cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        namedCmOp = mock(Resource.class);
        cmResourceOp = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.withName(QUORUM_CM_NAME)).thenReturn(namedCmOp);
        when(namedCmOp.get()).thenReturn(quorumConfigMap());
        when(nsCmOp.resource(any(ConfigMap.class))).thenReturn(cmResourceOp);
        when(nsCmOp.withName(POOL_NAME + "-config")).thenReturn(namedCmOp); // for cleanup

        // Services chain
        svcOp = mock(MixedOperation.class);
        nsSvcOp = mock(NonNamespaceOperation.class);
        svcResourceOp = mock(ServiceResource.class);
        namedSvcOp = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(Service.class))).thenReturn(svcResourceOp);
        when(nsSvcOp.withName(POOL_NAME + "-headless")).thenReturn(namedSvcOp);

        // KafkaPodSet: resource() for apply, withName() for status read
        podSetMixedOp = mock(MixedOperation.class);
        nsPodSetOp = mock(NonNamespaceOperation.class);
        podSetResourceOp = mock(Resource.class);
        namedPodSetOp = mock(Resource.class);
        when(client.resources(KafkaPodSet.class)).thenReturn(podSetMixedOp);
        when(podSetMixedOp.inNamespace(NS)).thenReturn(nsPodSetOp);
        when(nsPodSetOp.resource(any(KafkaPodSet.class))).thenReturn(podSetResourceOp);
        when(nsPodSetOp.withName(POD_SET_NAME)).thenReturn(namedPodSetOp);
        when(namedPodSetOp.get()).thenReturn(null); // default: podSet not found

        // Dependency stubs
        when(kraftConfig.clusterIndex(any(), anyString())).thenReturn(0);
        ConfigMap poolCm = new ConfigMap();
        poolCm.setData(Map.of("server.properties.template", "node.id=0\n"));
        when(poolConfigMapBuilder.build(any(), any(), anyString(), anyInt(), anyString(), anyString()))
                .thenReturn(poolCm);
        when(headlessServiceBuilder.build(any(), anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenReturn(mock(Service.class));
        when(headlessServiceBuilder.buildServiceExport(anyString(), anyString(), any()))
                .thenReturn(Optional.empty());
        when(podTemplateFactory.build(any(), any(), anyString(), anyInt(), anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenReturn(List.of());

        reconciler = new KafkaNodePoolReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "kraftConfig", kraftConfig);
        injectField(reconciler, "poolConfigMapBuilder", poolConfigMapBuilder);
        injectField(reconciler, "headlessServiceBuilder", headlessServiceBuilder);
        injectField(reconciler, "podTemplateFactory", podTemplateFactory);
        injectField(reconciler, "localClusterId", LOCAL_CLUSTER_ID);
        injectField(reconciler, "mcsEnabled", false);
    }

    @Test
    void validation_noRoles_setsFailedPhase() {
        KafkaNodePool pool = pool(List.of()); // empty roles

        reconciler.reconcile(pool, context);

        assertThat(pool.getStatus().getPhase()).isEqualTo(KafkaNodePoolStatus.Phase.FAILED);
        verify(namedClusterOp, never()).get();
    }

    @Test
    void reconcile_parentClusterNotFound_setsFailedPhase() {
        when(namedClusterOp.get()).thenReturn(null);
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        reconciler.reconcile(pool, context);

        assertThat(pool.getStatus().getPhase()).isEqualTo(KafkaNodePoolStatus.Phase.FAILED);
        assertThat(pool.getStatus().getMessage()).contains(CLUSTER_NAME);
    }

    @Test
    void reconcile_quorumCmNotReady_setsReconcilingPhase() {
        when(namedCmOp.get()).thenReturn(null);
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        reconciler.reconcile(pool, context);

        assertThat(pool.getStatus().getPhase()).isEqualTo(KafkaNodePoolStatus.Phase.RECONCILING);
        assertThat(pool.getStatus().getMessage()).contains("Quorum ConfigMap");
    }

    @Test
    void reconcile_happyPath_appliesConfigMapServiceAndPodSet() {
        KafkaPodSet readyPodSet = readyPodSet(1);
        when(namedPodSetOp.get()).thenReturn(readyPodSet);
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        reconciler.reconcile(pool, context);

        verify(cmResourceOp).serverSideApply();   // pool ConfigMap applied
        verify(svcResourceOp).serverSideApply();  // headless Service applied
        verify(podSetResourceOp).serverSideApply(); // KafkaPodSet applied
        verify(poolConfigMapBuilder).build(any(), any(), anyString(), anyInt(), anyString(), anyString());
        verify(podTemplateFactory).build(any(), any(), anyString(), anyInt(), anyString(), anyString(), anyBoolean(), anyBoolean());
        assertThat(pool.getStatus().getPhase()).isEqualTo(KafkaNodePoolStatus.Phase.READY);
    }

    @Test
    void cleanup_deletesConfigMapAndService() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        reconciler.cleanup(pool, context);

        verify(namedCmOp).delete();
        verify(namedSvcOp).delete();
    }

    // --- Helpers ---

    private KafkaNodePool pool(List<NodeRole> roles) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        meta.setNamespace(NS);
        meta.setLabels(Map.of(KafkaNodePool.CLUSTER_LABEL, CLUSTER_NAME));
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(roles);
        spec.setReplicas(1);
        pool.setSpec(spec);
        return pool;
    }

    private KafkaCluster validCluster() {
        KafkaCluster cluster = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(CLUSTER_NAME);
        cluster.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        ClusterEntry entry = new ClusterEntry();
        entry.setId(LOCAL_CLUSTER_ID);
        entry.setControllerAdvertisedAddress("ctrl-a.example.com:9093");
        spec.setClusters(List.of(entry));
        cluster.setSpec(spec);
        return cluster;
    }

    private ConfigMap quorumConfigMap() {
        ConfigMap cm = new ConfigMap();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(QUORUM_CM_NAME);
        cm.setMetadata(meta);
        cm.setData(Map.of(
                "controller.quorum.voters", "1000@ctrl-a:9093",
                "cluster.id", "test-cluster-id"
        ));
        return cm;
    }

    private KafkaPodSet readyPodSet(int replicas) {
        KafkaPodSet ps = new KafkaPodSet();
        KafkaPodSetStatus status = new KafkaPodSetStatus();
        status.setReadyReplicas(replicas);
        status.setReplicas(replicas);
        ps.setStatus(status);
        return ps;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
