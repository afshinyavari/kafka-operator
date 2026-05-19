package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ExternalAccessServiceBuilderTest {

    private static final String NS = "kafka";
    private static final String POOL_NAME = "brokers-a";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final int CLUSTER_INDEX = 0;

    private ExternalAccessServiceBuilder builder;
    private KubernetesClient client;
    private NonNamespaceOperation nsSvcOp;
    private ServiceResource svcResourceOp;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() {
        builder = new ExternalAccessServiceBuilder();

        client = mock(KubernetesClient.class);
        MixedOperation svcOp = mock(MixedOperation.class);
        nsSvcOp = mock(NonNamespaceOperation.class);
        svcResourceOp = mock(ServiceResource.class);

        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(Service.class))).thenReturn(svcResourceOp);
    }

    @Test
    void applyExternalServices_serviceNamePattern() {
        ArgumentCaptor<Service> captor = ArgumentCaptor.forClass(Service.class);
        KafkaListenerSpec l = listener("CLIENT", 9095, 31000);

        builder.applyExternalServices(pool(1), NS, CLUSTER_NAME, List.of(l), CLUSTER_INDEX, client);

        verify(nsSvcOp).resource(captor.capture());
        Service svc = captor.getValue();
        // ordinal=0, listener.name=CLIENT → brokers-a-0-client-ext
        assertThat(svc.getMetadata().getName()).isEqualTo(POOL_NAME + "-0-client-ext");
    }

    @Test
    void applyExternalServices_nodePortAssignment() {
        ArgumentCaptor<Service> captor = ArgumentCaptor.forClass(Service.class);
        KafkaListenerSpec l = listener("CLIENT", 9095, 31500);

        builder.applyExternalServices(pool(1), NS, CLUSTER_NAME, List.of(l), CLUSTER_INDEX, client);

        verify(nsSvcOp).resource(captor.capture());
        Service svc = captor.getValue();
        int nodePort = svc.getSpec().getPorts().get(0).getNodePort();
        assertThat(nodePort).isEqualTo(31500); // nodePortBase=31500 + ordinal=0
    }

    @Test
    void applyExternalServices_nodeIdInSelector() {
        ArgumentCaptor<Service> captor = ArgumentCaptor.forClass(Service.class);
        KafkaListenerSpec l = listener("CLIENT", 9095, 31000);

        // clusterIndex=1 → nodeId = 1 * 1000 + 0 = 1000
        builder.applyExternalServices(pool(1), NS, CLUSTER_NAME, List.of(l), 1, client);

        verify(nsSvcOp).resource(captor.capture());
        Service svc = captor.getValue();
        String nodeIdSelector = svc.getSpec().getSelector().get(KafkaPodSet.NODE_ID_LABEL);
        assertThat(nodeIdSelector).isEqualTo(String.valueOf(1 * KRaftConfigGenerator.BROKER_MULTIPLIER));
    }

    @Test
    void applyExternalServices_multipleListeners_createsOneServiceEach() {
        KafkaListenerSpec l1 = listener("CLIENT", 9095, 31000);
        KafkaListenerSpec l2 = listener("ADMIN", 9096, 32000);

        // 2 replicas × 2 listeners → 4 services
        builder.applyExternalServices(pool(2), NS, CLUSTER_NAME, List.of(l1, l2), CLUSTER_INDEX, client);

        verify(nsSvcOp, times(4)).resource(any(Service.class));
        verify(svcResourceOp, times(4)).serverSideApply();
    }

    @Test
    void deleteExternalServices_deletesExtSuffixServices() {
        // Setup: list returns 2 services, one with -ext suffix and one without
        Service extSvc = serviceWithName(POOL_NAME + "-0-client-ext");
        Service otherSvc = serviceWithName(POOL_NAME + "-headless");

        FilterWatchListDeletable labelOp = mock(FilterWatchListDeletable.class);
        FilterWatchListDeletable label2Op = mock(FilterWatchListDeletable.class);
        ServiceList list = mock(ServiceList.class);
        ServiceResource namedSvc = mock(ServiceResource.class);

        when(nsSvcOp.withLabel(KafkaPodSet.NODE_POOL_LABEL, POOL_NAME)).thenReturn(labelOp);
        when(labelOp.withLabel(KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE)).thenReturn(label2Op);
        when(label2Op.list()).thenReturn(list);
        when(list.getItems()).thenReturn(List.of(extSvc, otherSvc));
        when(nsSvcOp.withName(POOL_NAME + "-0-client-ext")).thenReturn(namedSvc);

        builder.deleteExternalServices(pool(1), NS, client);

        // Only the -ext service should be deleted
        verify(namedSvc).delete();
    }

    // --- helpers ---

    private KafkaNodePool pool(int replicas) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        meta.setNamespace(NS);
        meta.setUid("uid-1234");
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(List.of(NodeRole.BROKER));
        spec.setReplicas(replicas);
        pool.setSpec(spec);
        return pool;
    }

    private KafkaListenerSpec listener(String name, int port, int nodePortBase) {
        KafkaListenerSpec l = new KafkaListenerSpec();
        l.setName(name);
        l.setPort(port);
        l.setExternalAccess(ExternalAccessType.NODEPORT);
        l.setNodePortBase(nodePortBase);
        return l;
    }

    private Service serviceWithName(String name) {
        Service svc = new Service();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        svc.setMetadata(meta);
        return svc;
    }
}
