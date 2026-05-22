package se.afshin.yavari.kafka.operator.rebalance;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalance;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceMode;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceStatus.Phase;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlEndpoint;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlResponse;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlState;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.Status;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RebalanceStateMachineTest {

    private static final CruiseControlEndpoint EP =
            new CruiseControlEndpoint("http://cruise-control:9090", null, null);

    private CruiseControlClient cc;
    private RebalanceStateMachine sm;

    @BeforeEach
    void setup() throws Exception {
        cc = mock(CruiseControlClient.class);
        sm = new RebalanceStateMachine();
        setField(sm, "cc", cc);
        // 'client' (KubernetesClient) is left null — clearAnnotation tolerates it and
        // still removes the annotation from the in-memory CR.
    }

    @Test
    void newRequestsProposalAndMovesToPending() {
        when(cc.rebalance(eq(EP), any(), eq(true), isNull()))
                .thenReturn(new CruiseControlResponse("task-1", Status.IN_PROGRESS, Map.of()));
        KafkaRebalance cr = rebalance(Phase.NEW, null);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PENDING_PROPOSAL);
        assertThat(cr.getStatus().getSessionId()).isEqualTo("task-1");
    }

    @Test
    void newWithImmediateProposalGoesToProposalReady() {
        when(cc.rebalance(eq(EP), any(), eq(true), isNull()))
                .thenReturn(new CruiseControlResponse("task-1", Status.COMPLETED,
                        Map.of("dataToMoveMB", "100", "numReplicaMovements", "8")));
        KafkaRebalance cr = rebalance(Phase.NEW, null);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PROPOSAL_READY);
        assertThat(cr.getStatus().getOptimizationResult()).containsEntry("numReplicaMovements", "8");
        assertThat(cr.getStatus().getDataToMoveMB()).isEqualTo("100");
    }

    @Test
    void pendingProposalCompletesToProposalReady() {
        when(cc.rebalance(eq(EP), any(), eq(true), eq("task-1")))
                .thenReturn(new CruiseControlResponse("task-1", Status.COMPLETED,
                        Map.of("dataToMoveMB", "42")));
        KafkaRebalance cr = rebalance(Phase.PENDING_PROPOSAL, null);
        cr.getStatus().setSessionId("task-1");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PROPOSAL_READY);
    }

    @Test
    void pendingProposalStaysWhileInProgress() {
        when(cc.rebalance(eq(EP), any(), eq(true), eq("task-1")))
                .thenReturn(new CruiseControlResponse("task-1", Status.IN_PROGRESS, Map.of()));
        KafkaRebalance cr = rebalance(Phase.PENDING_PROPOSAL, null);
        cr.getStatus().setSessionId("task-1");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PENDING_PROPOSAL);
    }

    @Test
    void approveAnnotationTriggersExecution() {
        when(cc.rebalance(eq(EP), any(), eq(false), isNull()))
                .thenReturn(new CruiseControlResponse("exec-1", Status.IN_PROGRESS, Map.of()));
        KafkaRebalance cr = rebalance(Phase.PROPOSAL_READY, KafkaRebalance.APPROVE);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.REBALANCING);
        assertThat(cr.getStatus().getExecutionTaskId()).isEqualTo("exec-1");
        assertThat(cr.pendingAnnotation()).isNull();
    }

    @Test
    void refreshAnnotationResetsProposalReadyToNew() {
        KafkaRebalance cr = rebalance(Phase.PROPOSAL_READY, KafkaRebalance.REFRESH);
        cr.getStatus().setSessionId("stale");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.NEW);
        assertThat(cr.getStatus().getSessionId()).isNull();
    }

    @Test
    void proposalReadyWithoutAnnotationIsIdle() {
        KafkaRebalance cr = rebalance(Phase.PROPOSAL_READY, null);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PROPOSAL_READY);
        verifyNoInteractions(cc);
    }

    @Test
    void rebalancingCompletesWhenExecutorIdle() {
        when(cc.rebalance(eq(EP), any(), eq(false), eq("exec-1")))
                .thenReturn(new CruiseControlResponse("exec-1", Status.COMPLETED, Map.of()));
        when(cc.state(EP)).thenReturn(new CruiseControlState(true));
        KafkaRebalance cr = rebalance(Phase.REBALANCING, null);
        cr.getStatus().setExecutionTaskId("exec-1");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.READY);
    }

    @Test
    void rebalancingStaysWhileExecutorBusy() {
        when(cc.rebalance(eq(EP), any(), eq(false), eq("exec-1")))
                .thenReturn(new CruiseControlResponse("exec-1", Status.COMPLETED, Map.of()));
        when(cc.state(EP)).thenReturn(new CruiseControlState(false));
        KafkaRebalance cr = rebalance(Phase.REBALANCING, null);
        cr.getStatus().setExecutionTaskId("exec-1");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.REBALANCING);
    }

    @Test
    void stopAnnotationDuringRebalancingCallsStopExecution() {
        KafkaRebalance cr = rebalance(Phase.REBALANCING, KafkaRebalance.STOP);
        cr.getStatus().setExecutionTaskId("exec-1");

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.STOPPED);
        verify(cc).stopExecution(EP);
    }

    @Test
    void notEnoughWindowsKeepsRebalancePending() {
        // Cruise Control rejects the proposal while still collecting metric samples —
        // a transient warm-up condition, not a terminal failure.
        when(cc.rebalance(eq(EP), any(), eq(true), isNull()))
                .thenReturn(new CruiseControlResponse("task-1", Status.ERROR,
                        Map.of("message", "com.linkedin.cruisecontrol.exception."
                                + "NotEnoughValidWindowsException: only 0 valid windows")));
        KafkaRebalance cr = rebalance(Phase.NEW, null);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PENDING_PROPOSAL);
    }

    @Test
    void cruiseControlErrorMovesToNotReady() {
        when(cc.rebalance(any(), any(), eq(true), isNull()))
                .thenThrow(new CruiseControlException("connection refused"));
        KafkaRebalance cr = rebalance(Phase.NEW, null);

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.NOT_READY);
        assertThat(cr.getStatus().getMessage()).contains("connection refused");
    }

    @Test
    void addBrokersWithoutBrokerIdsFailsValidation() {
        KafkaRebalance cr = rebalance(Phase.NEW, null);
        cr.getSpec().setMode(KafkaRebalanceMode.ADD_BROKERS);
        cr.getSpec().setBrokers(List.of());

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.NOT_READY);
        assertThat(cr.getStatus().getMessage()).contains("spec.brokers");
        verifyNoInteractions(cc);
    }

    @Test
    void specChangeOnSettledProposalReProposes() {
        KafkaRebalance cr = rebalance(Phase.PROPOSAL_READY, null);
        cr.getStatus().setObservedGeneration(1L);
        cr.getMetadata().setGeneration(2L); // spec edited
        when(cc.rebalance(eq(EP), any(), eq(true), isNull()))
                .thenReturn(new CruiseControlResponse("task-2", Status.IN_PROGRESS, Map.of()));

        sm.advance(cr, EP);

        assertThat(cr.getStatus().getPhase()).isEqualTo(Phase.PENDING_PROPOSAL);
        assertThat(cr.getStatus().getObservedGeneration()).isEqualTo(2L);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static KafkaRebalance rebalance(Phase phase, String annotation) {
        KafkaRebalance cr = new KafkaRebalance();
        Map<String, String> annotations = new HashMap<>();
        if (annotation != null) {
            annotations.put(KafkaRebalance.REBALANCE_ANNOTATION, annotation);
        }
        cr.setMetadata(new ObjectMetaBuilder()
                .withName("rb1").withNamespace("kafka").withGeneration(1L)
                .withAnnotations(annotations)
                .build());
        KafkaRebalanceSpec spec = new KafkaRebalanceSpec();
        spec.setClusterRef("my-kafka");
        spec.setMode(KafkaRebalanceMode.FULL);
        cr.setSpec(spec);
        KafkaRebalanceStatus status = new KafkaRebalanceStatus();
        status.setPhase(phase);
        status.setObservedGeneration(1L);
        cr.setStatus(status);
        return cr;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
