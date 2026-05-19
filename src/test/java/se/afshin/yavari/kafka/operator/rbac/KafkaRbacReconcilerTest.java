package se.afshin.yavari.kafka.operator.rbac;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacStatus;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaRbacReconcilerTest {

    private static final String NS = "kafka";
    private static final String RBAC_NAME = "my-rbac";

    private KubernetesClient client;
    private KafkaRbacConfigMapBuilder configMapBuilder;
    private Context<KafkaRbac> context;
    private KafkaRbacReconciler reconciler;

    private NonNamespaceOperation nsCmOp;
    private Resource cmResource;

    @BeforeEach
    void setup() throws Exception {
        configMapBuilder = mock(KafkaRbacConfigMapBuilder.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        MixedOperation cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        cmResource = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.resource(any(ConfigMap.class))).thenReturn(cmResource);

        ConfigMap fakeCm = new ConfigMap();
        when(configMapBuilder.buildKafkaRules(any(), any())).thenReturn(fakeCm);
        when(configMapBuilder.buildApicurioPolicy(any(), any())).thenReturn(fakeCm);

        reconciler = new KafkaRbacReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "configMapBuilder", configMapBuilder);
    }

    @Test
    void reconcile_happyPath_appliesBothConfigMapsAndSetsReady() {
        KafkaRbac rbac = rbac();

        UpdateControl<KafkaRbac> result = reconciler.reconcile(rbac, context);

        verify(configMapBuilder).buildKafkaRules(any(), any());
        verify(configMapBuilder).buildApicurioPolicy(any(), any());
        verify(cmResource, times(2)).serverSideApply();
        assertThat(rbac.getStatus().getPhase()).isEqualTo(KafkaRbacStatus.Phase.READY);
        assertThat(rbac.getStatus().getMessage()).isNull();
    }

    @Test
    void reconcile_builderThrows_setsFailedWithMessage() {
        when(configMapBuilder.buildKafkaRules(any(), any()))
                .thenThrow(new RuntimeException("config build failed"));
        KafkaRbac rbac = rbac();

        reconciler.reconcile(rbac, context);

        assertThat(rbac.getStatus().getPhase()).isEqualTo(KafkaRbacStatus.Phase.FAILED);
        assertThat(rbac.getStatus().getMessage()).contains("config build failed");
    }

    @Test
    void cleanup_returnsDefaultDelete() {
        KafkaRbac rbac = rbac();
        DeleteControl result = reconciler.cleanup(rbac, context);
        assertThat(result.isRemoveFinalizer()).isTrue();
    }

    // --- helpers ---

    private KafkaRbac rbac() {
        KafkaRbac rbac = new KafkaRbac();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(RBAC_NAME);
        meta.setNamespace(NS);
        rbac.setMetadata(meta);
        KafkaRbacSpec spec = new KafkaRbacSpec();
        spec.setGroups(List.of());
        spec.setUsers(List.of());
        rbac.setSpec(spec);
        return rbac;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
