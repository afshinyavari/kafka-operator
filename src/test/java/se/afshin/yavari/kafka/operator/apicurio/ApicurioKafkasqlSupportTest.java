package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.acl.KafkaAclManager;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistrySpec;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicStatus;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ApicurioKafkasqlSupportTest {

    private static final String NS = "kafka";
    private static final String CLUSTER = "my-kafka";

    private KubernetesClient client;
    private BrokerBootstrapResolver bootstrapResolver;
    private AdminClientTlsLoader tlsLoader;
    private KafkaAclManager aclManager;
    private ApicurioKafkasqlSupport support;

    private Resource clusterResource;
    private Resource topicResource;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        bootstrapResolver = mock(BrokerBootstrapResolver.class);
        tlsLoader = mock(AdminClientTlsLoader.class);
        aclManager = mock(KafkaAclManager.class);

        MixedOperation clusterOp = mock(MixedOperation.class);
        NonNamespaceOperation nsClusterOp = mock(NonNamespaceOperation.class);
        clusterResource = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterOp);
        when(clusterOp.inNamespace(NS)).thenReturn(nsClusterOp);
        when(nsClusterOp.withName(anyString())).thenReturn(clusterResource);

        MixedOperation topicOp = mock(MixedOperation.class);
        NonNamespaceOperation nsTopicOp = mock(NonNamespaceOperation.class);
        topicResource = mock(Resource.class);
        when(client.resources(KafkaTopic.class)).thenReturn(topicOp);
        when(topicOp.inNamespace(NS)).thenReturn(nsTopicOp);
        when(nsTopicOp.resource(any(KafkaTopic.class))).thenReturn(topicResource);

        when(bootstrapResolver.resolve(anyString(), anyString())).thenReturn("b:9092");

        support = new ApicurioKafkasqlSupport();
        inject("client", client);
        inject("bootstrapResolver", bootstrapResolver);
        inject("tlsLoader", tlsLoader);
        inject("aclManager", aclManager);
    }

    @Test
    void prepare_failsWhenClusterRefMissing() {
        ApicurioRegistry r = registry(null, null, null);

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Failed.class);
        assertThat(((ApicurioKafkasqlSupport.Result.Failed) result).message()).contains("clusterRef");
    }

    @Test
    void prepare_failsWhenClusterMissing() {
        ApicurioRegistry r = registry(CLUSTER, null, null);
        when(clusterResource.get()).thenReturn(null);

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Failed.class);
        assertThat(((ApicurioKafkasqlSupport.Result.Failed) result).message()).contains("not found");
    }

    @Test
    void prepare_mtls_failsWhenTlsSecretRefMissing() {
        ApicurioRegistry r = registry(CLUSTER, null, "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Failed.class);
        assertThat(((ApicurioKafkasqlSupport.Result.Failed) result).message()).contains("tlsSecretRef");
    }

    @Test
    void prepare_mtls_failsWhenPrincipalMissing() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", null);
        when(clusterResource.get()).thenReturn(cluster(true));

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Failed.class);
        assertThat(((ApicurioKafkasqlSupport.Result.Failed) result).message()).contains("principal");
    }

    @Test
    void prepare_mtls_failsWhenSecretShapeBroken() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));
        org.mockito.Mockito.doThrow(new AdminClientTlsLoader.TlsSecretNotFoundException("missing ca.crt"))
                .when(tlsLoader).validateSecretShape(NS, "registry-tls");

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Failed.class);
        assertThat(((ApicurioKafkasqlSupport.Result.Failed) result).message()).contains("ca.crt");
    }

    @Test
    void prepare_topicNotReady_returnsPending_andDoesNotProvisionAcls() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));
        when(topicResource.serverSideApply()).thenReturn(topicWithPhase(KafkaTopicStatus.Phase.RECONCILING));

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Pending.class);
        verify(aclManager, never()).apply(anyString(), anyString(), anyString(), any());
    }

    @Test
    void prepare_topicReady_returnsReadyConfigAndApplyAcls() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));
        when(topicResource.serverSideApply()).thenReturn(topicWithPhase(KafkaTopicStatus.Phase.READY));

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Ready.class);
        var ready = (ApicurioKafkasqlSupport.Result.Ready) result;
        assertThat(ready.config().bootstrapServers()).isEqualTo("b:9092");
        assertThat(ready.config().topic()).isEqualTo("kafkasql-journal");
        assertThat(ready.config().tlsSecretRef()).isEqualTo("registry-tls");
        assertThat(ready.config().initImage()).isEqualTo("kafka-ubi:4.0.0");

        ArgumentCaptor<Collection<AclBinding>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(aclManager).apply(eq(CLUSTER), eq(NS), anyString(), captor.capture());
        Collection<AclBinding> acls = captor.getValue();
        assertThat(acls).extracting(b -> b.entry().principal())
                .containsOnly("User:CN=apicurio-registry");
        assertThat(acls).extracting(b -> b.pattern().resourceType())
                .contains(ResourceType.TOPIC, ResourceType.GROUP, ResourceType.CLUSTER);
        assertThat(acls).extracting(b -> b.entry().operation())
                .contains(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE);
    }

    @Test
    void prepare_noMtls_skipsAclProvisioning() {
        ApicurioRegistry r = registry(CLUSTER, null, null);
        when(clusterResource.get()).thenReturn(cluster(false));
        when(topicResource.serverSideApply()).thenReturn(topicWithPhase(KafkaTopicStatus.Phase.READY));

        var result = support.prepare(r);

        assertThat(result).isInstanceOf(ApicurioKafkasqlSupport.Result.Ready.class);
        var ready = (ApicurioKafkasqlSupport.Result.Ready) result;
        assertThat(ready.config().tlsSecretRef()).isNull();
        verify(aclManager, never()).apply(anyString(), anyString(), anyString(), any());
    }

    @Test
    void cleanup_mtls_deletesAclsByPrincipal() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));

        support.cleanup(r);

        verify(aclManager).delete(eq(CLUSTER), eq(NS), anyString(), eq("User:CN=apicurio-registry"));
    }

    @Test
    void cleanup_nonKafkasql_doesNothing() {
        ApicurioRegistry r = new ApicurioRegistry();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("x"); meta.setNamespace(NS);
        r.setMetadata(meta);
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
        storage.setType("mem");
        spec.setStorage(storage);
        r.setSpec(spec);

        support.cleanup(r);

        verify(aclManager, never()).delete(anyString(), anyString(), anyString(), anyString());
    }

    @Test
    void prepare_topicReady_topicCrIsServerSideApplied() {
        ApicurioRegistry r = registry(CLUSTER, "registry-tls", "apicurio-registry");
        when(clusterResource.get()).thenReturn(cluster(true));
        when(topicResource.serverSideApply()).thenReturn(topicWithPhase(KafkaTopicStatus.Phase.READY));

        support.prepare(r);

        // Verify that the auto-created KafkaTopic carried the right spec
        ArgumentCaptor<KafkaTopic> captor = ArgumentCaptor.forClass(KafkaTopic.class);
        verify(client.resources(KafkaTopic.class).inNamespace(NS), atLeastOnce()).resource(captor.capture());
        KafkaTopic created = captor.getValue();
        assertThat(created.getMetadata().getName()).isEqualTo("schema-registry-kafkasql-journal");
        assertThat(created.getSpec().getClusterRef()).isEqualTo(CLUSTER);
        assertThat(created.getSpec().getTopicName()).isEqualTo("kafkasql-journal");
        assertThat(created.getSpec().getPartitions()).isEqualTo(1);
        assertThat(created.getSpec().getReplicationFactor()).isEqualTo((short) 3);
        assertThat(created.getSpec().getConfig()).containsEntry("cleanup.policy", "compact");
        assertThat(created.getSpec().getConfig()).containsEntry("min.insync.replicas", "2");
        assertThat(created.getSpec().getDeletionPolicy())
                .isEqualTo(se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy.RETAIN);
    }

    // helpers

    private static ApicurioRegistry registry(String clusterRef, String tlsSecretRef, String principal) {
        ApicurioRegistry r = new ApicurioRegistry();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("schema-registry");
        meta.setNamespace(NS);
        meta.setUid("u-123");
        r.setMetadata(meta);
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
        storage.setType("kafkasql");
        storage.setClusterRef(clusterRef);
        storage.setKafkaTopic("kafkasql-journal");
        storage.setTlsSecretRef(tlsSecretRef);
        storage.setPrincipal(principal);
        spec.setStorage(storage);
        r.setSpec(spec);
        return r;
    }

    private static KafkaCluster cluster(boolean mtlsEnabled) {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta m = new ObjectMeta();
        m.setName(CLUSTER);
        m.setNamespace(NS);
        c.setMetadata(m);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(List.of(new ClusterEntry()));
        if (mtlsEnabled) {
            KafkaProxyMtlsConfig mtls = new KafkaProxyMtlsConfig();
            mtls.setEnabled(true);
            spec.setProxyMtls(mtls);
        }
        c.setSpec(spec);
        return c;
    }

    private static KafkaTopic topicWithPhase(KafkaTopicStatus.Phase phase) {
        KafkaTopic t = new KafkaTopic();
        ObjectMeta m = new ObjectMeta();
        m.setName("schema-registry-kafkasql-journal");
        m.setNamespace(NS);
        t.setMetadata(m);
        KafkaTopicStatus status = new KafkaTopicStatus();
        status.setPhase(phase);
        t.setStatus(status);
        return t;
    }

    private void inject(String name, Object value) {
        try {
            var f = ApicurioKafkasqlSupport.class.getDeclaredField(name);
            f.setAccessible(true);
            f.set(support, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
