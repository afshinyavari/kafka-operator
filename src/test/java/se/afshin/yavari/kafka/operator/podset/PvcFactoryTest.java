package se.afshin.yavari.kafka.operator.podset;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.crd.StorageSpec;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class PvcFactoryTest {

    private static final String NS = "kafka";
    private static final String POD_NAME = "broker-a-0";

    private KubernetesClient client;
    private MixedOperation pvcsOp;
    private NonNamespaceOperation nsPvcsOp;
    private Resource<PersistentVolumeClaim> namedPvcOp;
    private Resource<PersistentVolumeClaim> pvcResourceOp;
    private PvcFactory factory;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        pvcsOp = mock(MixedOperation.class);
        nsPvcsOp = mock(NonNamespaceOperation.class);
        namedPvcOp = mock(Resource.class);
        pvcResourceOp = mock(Resource.class);

        when(client.persistentVolumeClaims()).thenReturn(pvcsOp);
        when(pvcsOp.inNamespace(NS)).thenReturn(nsPvcsOp);
        when(nsPvcsOp.withName("data-" + POD_NAME)).thenReturn(namedPvcOp);
        when(nsPvcsOp.resource(any(PersistentVolumeClaim.class))).thenReturn(pvcResourceOp);

        factory = new PvcFactory();
        var field = PvcFactory.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(factory, client);
    }

    @Test
    void pvcAlreadyExists_doesNotCreate() {
        when(namedPvcOp.get()).thenReturn(mock(PersistentVolumeClaim.class));

        factory.ensure(entry(), NS, owner(), new StorageSpec());

        verify(pvcResourceOp, never()).create();
    }

    @Test
    void pvcMissing_createsWithSizeFromStorageSpec() {
        when(namedPvcOp.get()).thenReturn(null);
        ArgumentCaptor<PersistentVolumeClaim> captor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(nsPvcsOp.resource(captor.capture())).thenReturn(pvcResourceOp);

        StorageSpec storage = new StorageSpec();
        storage.setSize("50Gi");
        factory.ensure(entry(), NS, owner(), storage);

        verify(pvcResourceOp).create();
        String requestedSize = captor.getValue().getSpec().getResources()
                .getRequests().get("storage").toString();
        assertThat(requestedSize).isEqualTo("50Gi");
    }

    @Test
    void pvcMissing_nullStorageSpec_defaultsTo10Gi() {
        when(namedPvcOp.get()).thenReturn(null);
        ArgumentCaptor<PersistentVolumeClaim> captor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(nsPvcsOp.resource(captor.capture())).thenReturn(pvcResourceOp);

        factory.ensure(entry(), NS, owner(), null);

        verify(pvcResourceOp).create();
        String size = captor.getValue().getSpec().getResources()
                .getRequests().get("storage").toString();
        assertThat(size).isEqualTo("10Gi");
    }

    @Test
    void pvcMissing_withStorageClassName_includedInSpec() {
        when(namedPvcOp.get()).thenReturn(null);
        ArgumentCaptor<PersistentVolumeClaim> captor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(nsPvcsOp.resource(captor.capture())).thenReturn(pvcResourceOp);

        StorageSpec storage = new StorageSpec();
        storage.setSize("20Gi");
        storage.setStorageClassName("fast-ssd");
        factory.ensure(entry(), NS, owner(), storage);

        verify(pvcResourceOp).create();
        assertThat(captor.getValue().getSpec().getStorageClassName()).isEqualTo("fast-ssd");
    }

    @Test
    void pvcMissing_noStorageClassName_notSetInSpec() {
        when(namedPvcOp.get()).thenReturn(null);
        ArgumentCaptor<PersistentVolumeClaim> captor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(nsPvcsOp.resource(captor.capture())).thenReturn(pvcResourceOp);

        StorageSpec storage = new StorageSpec();
        storage.setSize("20Gi");
        // no storage class name set
        factory.ensure(entry(), NS, owner(), storage);

        verify(pvcResourceOp).create();
        assertThat(captor.getValue().getSpec().getStorageClassName()).isNull();
    }

    // --- Helpers ---

    private PodEntry entry() {
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POD_NAME);
        meta.setLabels(Map.of(KafkaPodSet.NODE_POOL_LABEL, "broker-pool"));
        PodEntry e = new PodEntry();
        e.setMetadata(meta);
        return e;
    }

    private KafkaPodSet owner() {
        KafkaPodSet ps = new KafkaPodSet();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("broker-pool-podset");
        meta.setUid("uid-123");
        ps.setMetadata(meta);
        return ps;
    }
}
