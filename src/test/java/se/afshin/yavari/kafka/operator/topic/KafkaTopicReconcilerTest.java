package se.afshin.yavari.kafka.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicStatus;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaTopicReconcilerTest {

    private static final String NS = "kafka";
    private static final String CR_NAME = "orders";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String LOCAL_CLUSTER = "a";

    private KubernetesClient client;
    private KafkaTopicService service;
    private TopicReconcileLeader leader;
    private BrokerBootstrapResolver bootstrapResolver;
    private AdminClientTlsLoader tlsLoader;
    private Context<KafkaTopic> ctx;
    private KafkaTopicReconciler reconciler;
    private AdminClient admin;

    private Resource clusterResource;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        service = mock(KafkaTopicService.class);
        leader = mock(TopicReconcileLeader.class);
        bootstrapResolver = mock(BrokerBootstrapResolver.class);
        tlsLoader = mock(AdminClientTlsLoader.class);
        ctx = mock(Context.class);
        admin = mock(AdminClient.class);

        MixedOperation clusterOp = mock(MixedOperation.class);
        NonNamespaceOperation nsClusterOp = mock(NonNamespaceOperation.class);
        clusterResource = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterOp);
        when(clusterOp.inNamespace(NS)).thenReturn(nsClusterOp);
        when(nsClusterOp.withName(anyString())).thenReturn(clusterResource);

        when(service.newAdmin(anyString())).thenReturn(admin);
        when(service.newAdmin(anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(admin);
        when(bootstrapResolver.resolve(anyString(), anyString()))
                .thenReturn("broker-headless." + NS + ".svc.cluster.local:9092");

        reconciler = new KafkaTopicReconciler();
        inject("client", client);
        inject("service", service);
        inject("leader", leader);
        inject("bootstrapResolver", bootstrapResolver);
        inject("tlsLoader", tlsLoader);
        inject("localClusterId", LOCAL_CLUSTER);
    }

    @Test
    void reconcile_missingCluster_setsFailed() {
        when(clusterResource.get()).thenReturn(null);

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.FAILED);
        assertThat(t.getStatus().getMessage()).contains("KafkaCluster");
    }

    @Test
    void reconcile_notLeader_setsSkippedWithoutAdminClient() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("b", "a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(false);
        when(leader.currentLeaderId(any())).thenReturn("b");

        reconciler.reconcile(topic(), ctx);

        verify(service, never()).newAdmin(anyString());
        verify(service, never()).newAdmin(anyString(), any());
    }

    @Test
    void reconcile_proxyMtlsEnabled_loadsTlsAndPassesToAdminClient() throws Exception {
        KafkaCluster cluster = cluster("a");
        KafkaProxyMtlsConfig mtls = new KafkaProxyMtlsConfig();
        mtls.setEnabled(true);
        cluster.getSpec().setProxyMtls(mtls);
        when(clusterResource.get()).thenReturn(cluster);
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        java.util.Properties sslProps = new java.util.Properties();
        sslProps.put("security.protocol", "SSL");
        when(tlsLoader.loadAsAdminClientSslProps(NS, KafkaProxyMtlsConfig.DEFAULT_ADMIN_CLIENT_CERT_SECRET))
                .thenReturn(sslProps);
        when(service.describe(eq(admin), eq("orders"))).thenReturn(null);
        when(service.createTopic(eq(admin), eq("orders"), anyInt(), anyShort(), any()))
                .thenReturn(new KafkaTopicService.TopicState("u", 3, (short) 1, java.util.Map.of()));

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        verify(service).newAdmin(anyString(), eq(sslProps));
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.READY);
    }

    @Test
    void reconcile_proxyMtlsEnabled_secretMissing_failsFast() throws Exception {
        KafkaCluster cluster = cluster("a");
        KafkaProxyMtlsConfig mtls = new KafkaProxyMtlsConfig();
        mtls.setEnabled(true);
        cluster.getSpec().setProxyMtls(mtls);
        when(clusterResource.get()).thenReturn(cluster);
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(tlsLoader.loadAsAdminClientSslProps(anyString(), anyString()))
                .thenThrow(new AdminClientTlsLoader.TlsSecretNotFoundException(
                        "TLS secret kafka/kafka-operator-client-tls not found"));

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        verify(service, never()).newAdmin(anyString(), any());
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.FAILED);
        assertThat(t.getStatus().getMessage()).contains("TLS secret");
    }

    @Test
    void reconcile_topicMissing_createsTopicAndSetsReady() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a", "b"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders"))).thenReturn(null);
        when(service.createTopic(eq(admin), eq("orders"), anyInt(), anyShort(), any()))
                .thenReturn(new KafkaTopicService.TopicState("topic-uuid", 3, (short) 1, Map.of()));

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        verify(service).createTopic(eq(admin), eq("orders"), eq(3), eq((short) 1), any());
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.READY);
        assertThat(t.getStatus().getTopicId()).isEqualTo("topic-uuid");
        assertThat(t.getStatus().getObservedPartitions()).isEqualTo(3);
    }

    @Test
    void reconcile_rfMismatch_setsFailedAndDoesNotAlter() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders")))
                .thenReturn(new KafkaTopicService.TopicState("uuid", 3, (short) 3, Map.of()));

        KafkaTopic t = topic(); // spec RF=1, topic RF=3 → mismatch
        reconciler.reconcile(t, ctx);

        verify(service, never()).increasePartitions(any(), anyString(), anyInt());
        verify(service, never()).applyConfigDiff(any(), anyString(), any());
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.FAILED);
        assertThat(t.getStatus().getMessage()).contains("ReplicationFactor mismatch");
    }

    @Test
    void reconcile_partitionDecrease_setsFailed() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders")))
                .thenReturn(new KafkaTopicService.TopicState("uuid", 10, (short) 1, Map.of()));
        when(service.computePartitionAction(anyInt(), anyInt()))
                .thenReturn(KafkaTopicService.PartitionAction.REJECT_DECREASE);

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        verify(service, never()).increasePartitions(any(), anyString(), anyInt());
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.FAILED);
        assertThat(t.getStatus().getMessage()).contains("Partition decrease");
    }

    @Test
    void reconcile_partitionExpansion_callsIncreasePartitions() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders")))
                .thenReturn(new KafkaTopicService.TopicState("uuid", 1, (short) 1, Map.of()),
                        new KafkaTopicService.TopicState("uuid", 3, (short) 1, Map.of()));
        when(service.computePartitionAction(1, 3))
                .thenReturn(KafkaTopicService.PartitionAction.EXPAND);
        when(service.computeConfigDiff(any(), any())).thenReturn(List.of());

        KafkaTopic t = topic();
        reconciler.reconcile(t, ctx);

        verify(service).increasePartitions(admin, "orders", 3);
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.READY);
        assertThat(t.getStatus().getObservedPartitions()).isEqualTo(3);
    }

    @Test
    void reconcile_configDrift_appliesDiff() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders")))
                .thenReturn(new KafkaTopicService.TopicState("uuid", 3, (short) 1, Map.of("retention.ms", "100")));
        when(service.computePartitionAction(3, 3))
                .thenReturn(KafkaTopicService.PartitionAction.NONE);
        var ops = List.of(new AlterConfigOp(
                new org.apache.kafka.clients.admin.ConfigEntry("retention.ms", "200"),
                AlterConfigOp.OpType.SET));
        when(service.computeConfigDiff(any(), any())).thenReturn(ops);

        KafkaTopic t = topic();
        t.getSpec().setPartitions(3);
        t.getSpec().setConfig(Map.of("retention.ms", "200"));
        reconciler.reconcile(t, ctx);

        verify(service).applyConfigDiff(admin, "orders", ops);
        assertThat(t.getStatus().getPhase()).isEqualTo(KafkaTopicStatus.Phase.READY);
    }

    @Test
    void reconcile_usesSpecTopicNameOverride() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        when(service.describe(eq(admin), eq("orders.v2"))).thenReturn(null);
        when(service.createTopic(eq(admin), eq("orders.v2"), anyInt(), anyShort(), any()))
                .thenReturn(new KafkaTopicService.TopicState("u", 1, (short) 1, Map.of()));

        KafkaTopic t = topic();
        t.getSpec().setTopicName("orders.v2");
        reconciler.reconcile(t, ctx);

        verify(service).createTopic(eq(admin), eq("orders.v2"), anyInt(), anyShort(), any());
    }

    @Test
    void cleanup_retainPolicy_doesNotDeleteTopic() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        KafkaTopic t = topic();
        t.getSpec().setDeletionPolicy(TopicDeletionPolicy.RETAIN);

        DeleteControl result = reconciler.cleanup(t, ctx);

        verify(service, never()).deleteTopic(any(), anyString());
        assertThat(result.isRemoveFinalizer()).isTrue();
    }

    @Test
    void cleanup_deletePolicyAsLeader_callsDeleteTopic() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);

        reconciler.cleanup(topic(), ctx);

        verify(service, times(1)).deleteTopic(admin, "orders");
    }

    @Test
    void cleanup_notLeader_releasesFinalizerWithoutDeletingTopic() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("b", "a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(false);

        DeleteControl result = reconciler.cleanup(topic(), ctx);

        verify(service, never()).deleteTopic(any(), anyString());
        assertThat(result.isRemoveFinalizer()).isTrue();
    }

    @Test
    void cleanup_kafkaUnreachable_keepsFinalizerForRetry() throws Exception {
        when(clusterResource.get()).thenReturn(cluster("a"));
        when(leader.isLeader(any(), any(), eq(LOCAL_CLUSTER))).thenReturn(true);
        org.mockito.Mockito.doThrow(new RuntimeException("connection refused"))
                .when(service).deleteTopic(any(), anyString());

        DeleteControl result = reconciler.cleanup(topic(), ctx);

        assertThat(result.isRemoveFinalizer()).isFalse();
    }

    // --- helpers ---

    private KafkaTopic topic() {
        KafkaTopic t = new KafkaTopic();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(CR_NAME);
        meta.setNamespace(NS);
        meta.setGeneration(1L);
        t.setMetadata(meta);
        KafkaTopicSpec spec = new KafkaTopicSpec();
        spec.setClusterRef(CLUSTER_NAME);
        spec.setPartitions(3);
        spec.setReplicationFactor((short) 1);
        spec.setDeletionPolicy(TopicDeletionPolicy.DELETE);
        t.setSpec(spec);
        return t;
    }

    private KafkaCluster cluster(String... ids) {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(CLUSTER_NAME);
        meta.setNamespace(NS);
        c.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(java.util.Arrays.stream(ids).map(id -> {
            ClusterEntry e = new ClusterEntry();
            e.setId(id);
            return e;
        }).toList());
        c.setSpec(spec);
        return c;
    }

    private void inject(String name, Object value) throws Exception {
        var field = KafkaTopicReconciler.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(reconciler, value);
    }
}
