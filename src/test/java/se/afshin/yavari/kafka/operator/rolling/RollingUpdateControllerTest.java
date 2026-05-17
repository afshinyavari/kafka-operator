package se.afshin.yavari.kafka.operator.rolling;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.metrics.OperatorMetrics;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class RollingUpdateControllerTest {

    private static final String NS = "kafka";
    private static final String POOL = "broker-pool";
    private static final String POD_NAME = "broker-a-0";
    private static final String BOOTSTRAP = "broker-headless.kafka.svc.cluster.local:9092";
    private static final int NODE_ID = 0;

    private IsrChecker isrChecker;
    private OperatorMetrics metrics;
    private Timer timer;

    // Explicit mock chain — avoids deep-stub generic resolution issues with fabric8
    private KubernetesClient client;
    private MixedOperation<Pod, PodList, PodResource> podsOp;
    private NonNamespaceOperation<Pod, PodList, PodResource> nsOp;
    private PodResource namedPodOp;
    private PodResource podResourceOp;

    private RollingUpdateController controller;

    @BeforeEach
    void setup() throws Exception {
        isrChecker = mock(IsrChecker.class);
        metrics = mock(OperatorMetrics.class);
        timer = mock(Timer.class);

        client = mock(KubernetesClient.class);
        podsOp = mock(MixedOperation.class);
        nsOp = mock(NonNamespaceOperation.class);
        namedPodOp = mock(PodResource.class);
        podResourceOp = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace(NS)).thenReturn(nsOp);
        when(nsOp.withName(POD_NAME)).thenReturn(namedPodOp);
        when(nsOp.resource(any(Pod.class))).thenReturn(podResourceOp);

        // Pod is gone immediately after delete — no sleep in waitForPodGone
        when(namedPodOp.get()).thenReturn(null);
        when(podResourceOp.create()).thenReturn(mock(Pod.class));
        when(metrics.rollingUpdateTimer(anyString(), anyString())).thenReturn(timer);

        controller = new RollingUpdateController();
        injectField(controller, "isrChecker", isrChecker);
        injectField(controller, "metrics", metrics);
    }

    @Test
    void rollPod_broker_completesAllSteps() {
        when(isrChecker.isBrokerSafeToRestart(BOOTSTRAP, NODE_ID)).thenReturn(true);
        stubWatchFiresReady();

        controller.rollPod(podEntry(), List.of(NodeRole.BROKER), BOOTSTRAP, NODE_ID, client, NS);

        // ISR safety checked before and after the roll
        verify(isrChecker, times(2)).isBrokerSafeToRestart(BOOTSTRAP, NODE_ID);
        // Old pod removed, new pod created
        verify(namedPodOp).delete();
        verify(podResourceOp).create();
        // Metrics reflect success
        verify(metrics).recordRollingUpdate(NS, POOL, true);
        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void rollPod_controller_callsControllerIsrCheck() {
        when(isrChecker.isControllerSafeToRestart(BOOTSTRAP, NODE_ID)).thenReturn(true);
        stubWatchFiresReady();

        controller.rollPod(podEntry(), List.of(NodeRole.CONTROLLER), BOOTSTRAP, NODE_ID, client, NS);

        verify(isrChecker, times(2)).isControllerSafeToRestart(BOOTSTRAP, NODE_ID);
        verify(isrChecker, never()).isBrokerSafeToRestart(any(), anyInt());
    }

    @Test
    void rollPod_combinedRoles_usesControllerCheck() {
        when(isrChecker.isControllerSafeToRestart(BOOTSTRAP, NODE_ID)).thenReturn(true);
        stubWatchFiresReady();

        controller.rollPod(podEntry(), List.of(NodeRole.CONTROLLER, NodeRole.BROKER), BOOTSTRAP, NODE_ID, client, NS);

        verify(isrChecker, times(2)).isControllerSafeToRestart(BOOTSTRAP, NODE_ID);
        verify(isrChecker, never()).isBrokerSafeToRestart(any(), anyInt());
    }

    @Test
    void rollPod_isrCheckThrows_recordsFailureMetric() {
        when(isrChecker.isBrokerSafeToRestart(BOOTSTRAP, NODE_ID))
                .thenThrow(new RuntimeException("Kafka admin API unreachable"));

        assertThatThrownBy(() ->
                controller.rollPod(podEntry(), List.of(NodeRole.BROKER), BOOTSTRAP, NODE_ID, client, NS))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("unreachable");

        verify(metrics).recordRollingUpdate(NS, POOL, false);
        verify(timer).record(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void rollPod_podReadyAfterNonReadyEvent_completesSuccessfully() {
        when(isrChecker.isBrokerSafeToRestart(BOOTSTRAP, NODE_ID)).thenReturn(true);
        // Watch fires Ready=False first, then Ready=True — must wait for the second event
        when(namedPodOp.watch(any())).thenAnswer(inv -> {
            Watcher<Pod> watcher = inv.getArgument(0);
            Thread t = new Thread(() -> {
                try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                watcher.eventReceived(Watcher.Action.MODIFIED, pod(false));
                watcher.eventReceived(Watcher.Action.MODIFIED, pod(true));
            });
            t.setDaemon(true);
            t.start();
            return mock(Watch.class);
        });

        controller.rollPod(podEntry(), List.of(NodeRole.BROKER), BOOTSTRAP, NODE_ID, client, NS);

        verify(metrics).recordRollingUpdate(NS, POOL, true);
    }

    @Test
    void rollPod_poolLabelMissing_usesUnknownInMetrics() {
        when(isrChecker.isBrokerSafeToRestart(BOOTSTRAP, NODE_ID)).thenReturn(true);
        stubWatchFiresReady();

        PodEntry entry = podEntry();
        entry.getMetadata().setLabels(Map.of()); // no pool label

        controller.rollPod(entry, List.of(NodeRole.BROKER), BOOTSTRAP, NODE_ID, client, NS);

        verify(metrics).recordRollingUpdate(NS, "unknown", true);
    }

    // --- Helpers ---

    private void stubWatchFiresReady() {
        when(namedPodOp.watch(any())).thenAnswer(inv -> {
            Watcher<Pod> watcher = inv.getArgument(0);
            Thread t = new Thread(() -> {
                try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                watcher.eventReceived(Watcher.Action.MODIFIED, pod(true));
            });
            t.setDaemon(true);
            t.start();
            return mock(Watch.class);
        });
    }

    private PodEntry podEntry() {
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POD_NAME);
        meta.setLabels(Map.of(KafkaPodSet.NODE_POOL_LABEL, POOL));
        PodEntry entry = new PodEntry();
        entry.setMetadata(meta);
        entry.setSpec(new PodSpec());
        return entry;
    }

    private Pod pod(boolean ready) {
        PodCondition condition = new PodCondition();
        condition.setType("Ready");
        condition.setStatus(ready ? "True" : "False");
        PodStatus status = new PodStatus();
        status.setConditions(List.of(condition));
        Pod p = new Pod();
        p.setStatus(status);
        return p;
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
