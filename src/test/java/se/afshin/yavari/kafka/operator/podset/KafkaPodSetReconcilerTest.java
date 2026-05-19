package se.afshin.yavari.kafka.operator.podset;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.metrics.OperatorMetrics;
import se.afshin.yavari.kafka.operator.rolling.IsrChecker;
import se.afshin.yavari.kafka.operator.rolling.RollingUpdateController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaPodSetReconcilerTest {

    private static final String NS = "kafka";
    private static final String POOL = "broker-a";
    private static final String POD_SET_NAME = POOL + "-podset";
    private static final String POD_NAME = POOL + "-0";
    private static final String EXCESS_POD = POOL + "-1";
    private static final String HASH_OLD = "aabbccdd00112233";
    private static final String HASH_NEW = "11223344aabbccdd";

    // Client mock chain for pods
    private KubernetesClient client;
    private MixedOperation<Pod, PodList, PodResource> podsOp;
    private NonNamespaceOperation<Pod, PodList, PodResource> nsPodsOp;
    private NonNamespaceOperation<Pod, PodList, PodResource> labeledPodsOp;
    private PodResource namedPodOp;
    private PodResource podResourceOp;

    // Client mock chain for KafkaNodePool lookup
    private MixedOperation npMixedOp;
    private NonNamespaceOperation nsNpOp;
    private Resource<KafkaNodePool> namedNpOp;

    private RollingUpdateController rollingController;
    private IsrChecker isrChecker;
    private PodSpecHasher podSpecHasher;
    private PvcFactory pvcFactory;
    private OperatorMetrics metrics;
    private Context<KafkaPodSet> context;
    private KafkaPodSetReconciler reconciler;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);

        // Pods chain
        podsOp = mock(MixedOperation.class);
        nsPodsOp = mock(NonNamespaceOperation.class);
        labeledPodsOp = mock(NonNamespaceOperation.class);
        namedPodOp = mock(PodResource.class);
        podResourceOp = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace(NS)).thenReturn(nsPodsOp);
        when(nsPodsOp.withLabels(any(Map.class))).thenReturn(labeledPodsOp);
        when(nsPodsOp.withName(anyString())).thenReturn(namedPodOp);
        when(nsPodsOp.resource(any(Pod.class))).thenReturn(podResourceOp);
        when(namedPodOp.get()).thenReturn(null); // default: pod not found on refresh

        // KafkaNodePool lookup chain (resolveRoles + resolveStorage)
        npMixedOp = mock(MixedOperation.class);
        nsNpOp = mock(NonNamespaceOperation.class);
        namedNpOp = mock(Resource.class);

        when(client.resources(KafkaNodePool.class)).thenReturn(npMixedOp);
        when(npMixedOp.inNamespace(NS)).thenReturn(nsNpOp);
        when(nsNpOp.withName(POOL)).thenReturn(namedNpOp);
        when(namedNpOp.get()).thenReturn(brokerNodePool());

        rollingController = mock(RollingUpdateController.class);
        isrChecker = mock(IsrChecker.class);
        podSpecHasher = mock(PodSpecHasher.class);
        pvcFactory = mock(PvcFactory.class);
        metrics = mock(OperatorMetrics.class);
        context = mock(Context.class);

        reconciler = new KafkaPodSetReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "rollingController", rollingController);
        injectField(reconciler, "isrChecker", isrChecker);
        injectField(reconciler, "podSpecHasher", podSpecHasher);
        injectField(reconciler, "pvcFactory", pvcFactory);
        injectField(reconciler, "metrics", metrics);
    }

    @Test
    void scaleUp_podMissing_createsPodAndEnsuresPvc() {
        when(podSpecHasher.hash(any())).thenReturn(HASH_NEW);
        stubActualPods(List.of()); // no existing pods

        reconciler.reconcile(podSet(List.of(desiredEntry(POD_NAME))), context);

        verify(pvcFactory).ensure(any(), anyString(), any(), any());
        verify(podResourceOp).create();
    }

    @Test
    void scaleDown_safe_deletesPodAndRecordsMetric() {
        when(isrChecker.isBrokerSafeToRestart(anyString(), anyInt())).thenReturn(true);
        when(podSpecHasher.hash(any())).thenReturn(HASH_OLD);
        // desired: pod-0 only; actual: pod-0 and pod-1 (excess)
        stubActualPods(List.of(actualPod(POD_NAME, HASH_OLD, true), actualPod(EXCESS_POD, HASH_OLD, true)));
        when(nsPodsOp.withName(EXCESS_POD)).thenReturn(namedPodOp);

        reconciler.reconcile(podSet(List.of(desiredEntry(POD_NAME))), context);

        verify(namedPodOp).delete();
        verify(metrics).recordScaleDown(anyString(), anyString(), eq(true));
    }

    @Test
    void scaleDown_unsafe_keepsPodAndRecordsMetric() {
        when(isrChecker.isBrokerSafeToRestart(anyString(), anyInt())).thenReturn(false);
        when(podSpecHasher.hash(any())).thenReturn(HASH_OLD);
        stubActualPods(List.of(actualPod(POD_NAME, HASH_OLD, true), actualPod(EXCESS_POD, HASH_OLD, true)));

        reconciler.reconcile(podSet(List.of(desiredEntry(POD_NAME))), context);

        verify(namedPodOp, never()).delete();
        verify(metrics).recordScaleDown(anyString(), anyString(), eq(false));
    }

    @Test
    void rollingUpdate_hashMismatch_callsRollingController() {
        when(podSpecHasher.hash(any())).thenReturn(HASH_NEW);
        // actual pod has old hash annotation
        stubActualPods(List.of(actualPod(POD_NAME, HASH_OLD, true)));

        reconciler.reconcile(podSet(List.of(desiredEntry(POD_NAME))), context);

        verify(rollingController).rollPod(any(), any(), anyString(), anyInt(), any(), anyString());
    }

    @Test
    void noChanges_hashMatch_doesNotRoll() {
        when(podSpecHasher.hash(any())).thenReturn(HASH_OLD);
        // actual pod has same hash annotation → no change needed
        stubActualPods(List.of(actualPod(POD_NAME, HASH_OLD, true)));

        reconciler.reconcile(podSet(List.of(desiredEntry(POD_NAME))), context);

        verify(rollingController, never()).rollPod(any(), any(), anyString(), anyInt(), any(), anyString());
        verify(podResourceOp, never()).create();
        verify(namedPodOp, never()).delete();
    }

    // --- Helpers ---

    private void stubActualPods(List<Pod> pods) {
        PodList podList = new PodList();
        podList.setItems(pods);
        when(labeledPodsOp.list()).thenReturn(podList);
    }

    private KafkaPodSet podSet(List<PodEntry> desiredPods) {
        KafkaPodSet ps = new KafkaPodSet();

        ObjectMeta meta = new ObjectMeta();
        meta.setName(POD_SET_NAME);
        meta.setNamespace(NS);
        meta.setLabels(Map.of(KafkaPodSet.NODE_POOL_LABEL, POOL));
        ps.setMetadata(meta);

        LabelSelector selector = new LabelSelector();
        selector.setMatchLabels(Map.of(KafkaPodSet.NODE_POOL_LABEL, POOL));

        KafkaPodSetSpec spec = new KafkaPodSetSpec();
        spec.setSelector(selector);
        spec.setPods(desiredPods);
        ps.setSpec(spec);

        return ps;
    }

    private PodEntry desiredEntry(String podName) {
        ObjectMeta meta = new ObjectMeta();
        meta.setName(podName);
        meta.setNamespace(NS);
        meta.setLabels(new HashMap<>(Map.of(KafkaPodSet.NODE_POOL_LABEL, POOL)));
        PodEntry e = new PodEntry();
        e.setMetadata(meta);
        e.setSpec(new PodSpec());
        return e;
    }

    private Pod actualPod(String name, String specHash, boolean ready) {
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        meta.setNamespace(NS);
        meta.setLabels(Map.of(
                KafkaPodSet.NODE_POOL_LABEL, POOL,
                KafkaPodSet.NODE_ID_LABEL, "0"
        ));
        meta.setAnnotations(Map.of(KafkaPodSet.SPEC_HASH_ANNOTATION, specHash));

        PodCondition readyCondition = new PodCondition();
        readyCondition.setType("Ready");
        readyCondition.setStatus(ready ? "True" : "False");
        PodStatus status = new PodStatus();
        status.setConditions(List.of(readyCondition));

        Pod pod = new Pod();
        pod.setMetadata(meta);
        pod.setStatus(status);
        pod.setSpec(new PodSpec());
        return pod;
    }

    private KafkaNodePool brokerNodePool() {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL);
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(List.of(NodeRole.BROKER));
        pool.setSpec(spec);
        return pool;
    }

    private static <T> T eq(T value) {
        return org.mockito.Mockito.eq(value);
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
