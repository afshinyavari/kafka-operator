package se.afshin.yavari.kafka.operator.rebalance;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.Condition;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalance;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceMode;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceStatus.Phase;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlEndpoint;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlResponse;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlState;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.Status;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * The {@link KafkaRebalance} state machine. Holds every transition so
 * {@link KafkaRebalanceReconciler} stays thin (CLAUDE.md rule 2).
 *
 * <p>{@code NEW → PENDING_PROPOSAL → PROPOSAL_READY →(annotate approve)→ REBALANCING → READY},
 * plus {@code STOPPED} and {@code NOT_READY}. Only the two polling phases reschedule; settled
 * phases are re-triggered by the approval annotation or a spec edit.
 */
@ApplicationScoped
public class RebalanceStateMachine {

    private static final Logger LOG = Logger.getLogger(RebalanceStateMachine.class);

    /** Quick tick to drive a freshly (re)started proposal. */
    static final Duration FAST = Duration.ofSeconds(5);
    /** Poll cadence while a Cruise Control task runs. */
    static final Duration POLL = Duration.ofSeconds(15);

    private static final String WARMUP_MESSAGE =
            "Waiting for Cruise Control to collect enough metric windows";

    /** Phases from which a spec edit re-proposes from scratch. */
    private static final Set<Phase> SETTLED =
            Set.of(Phase.PROPOSAL_READY, Phase.READY, Phase.STOPPED, Phase.NOT_READY);

    @Inject CruiseControlClient cc;
    @Inject KubernetesClient client;

    /** Advances {@code cr} by exactly one transition against the resolved Cruise Control endpoint. */
    public UpdateControl<KafkaRebalance> advance(KafkaRebalance cr, CruiseControlEndpoint endpoint) {
        KafkaRebalanceStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaRebalanceStatus();
        cr.setStatus(status);
        status.setLastReconcileTime(Instant.now().toString());

        String validationError = validate(cr.getSpec());
        if (validationError != null) {
            return fail(cr, status, validationError);
        }

        Long currentGen = cr.getMetadata().getGeneration();
        Long observedGen = status.getObservedGeneration();
        Phase phase = status.getPhase() == null ? Phase.NEW : status.getPhase();
        String annotation = cr.pendingAnnotation();

        // Generation guard — a spec edit on a settled CR discards the stale proposal.
        if (observedGen != null && !observedGen.equals(currentGen) && SETTLED.contains(phase)) {
            LOG.infof("KafkaRebalance %s/%s spec changed (gen %d→%d) — re-proposing",
                    cr.getMetadata().getNamespace(), cr.getMetadata().getName(), observedGen, currentGen);
            phase = Phase.NEW;
            resetProposalState(status);
        }
        status.setObservedGeneration(currentGen);

        RebalanceParams params = RebalanceParams.from(cr.getSpec());
        try {
            return switch (phase) {
                case NEW              -> onNew(cr, status, endpoint, params);
                case PENDING_PROPOSAL -> onPendingProposal(cr, status, endpoint, params);
                case PROPOSAL_READY   -> onProposalReady(cr, status, endpoint, params, annotation);
                case REBALANCING      -> onRebalancing(cr, status, endpoint, params, annotation);
                case READY, STOPPED   -> onSettled(cr, status, annotation);
                case NOT_READY        -> onNotReady(cr, status, annotation);
            };
        } catch (CruiseControlException e) {
            LOG.warnf("KafkaRebalance %s/%s: %s",
                    cr.getMetadata().getNamespace(), cr.getMetadata().getName(), e.getMessage());
            return fail(cr, status, "Cruise Control error: " + e.getMessage());
        }
    }

    // ── Phase handlers ──────────────────────────────────────────────────────────

    private UpdateControl<KafkaRebalance> onNew(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                CruiseControlEndpoint ep, RebalanceParams params) {
        CruiseControlResponse r = cc.rebalance(ep, params, true, null);
        if (r.status() == Status.ERROR) {
            if (isWarmingUp(r)) {
                status.setSessionId(null);
                return transition(cr, status, Phase.PENDING_PROPOSAL, WARMUP_MESSAGE, POLL);
            }
            return fail(cr, status, "Optimization proposal rejected: " + detail(r));
        }
        status.setSessionId(r.userTaskId());
        if (r.status() == Status.COMPLETED) {
            applyProposal(status, r);
            return transition(cr, status, Phase.PROPOSAL_READY, proposalReadyMessage(), null);
        }
        return transition(cr, status, Phase.PENDING_PROPOSAL,
                "Optimization proposal requested from Cruise Control", POLL);
    }

    private UpdateControl<KafkaRebalance> onPendingProposal(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                           CruiseControlEndpoint ep, RebalanceParams params) {
        CruiseControlResponse r = cc.rebalance(ep, params, true, status.getSessionId());
        if (r.status() == Status.ERROR) {
            // "Not enough valid windows" / "not enough snapshots" — Cruise Control is still
            // collecting metric samples. Stay pending and retry rather than failing.
            if (isWarmingUp(r)) {
                status.setSessionId(null);
                return transition(cr, status, Phase.PENDING_PROPOSAL, WARMUP_MESSAGE, POLL);
            }
            return fail(cr, status, "Optimization proposal failed: " + detail(r));
        }
        if (r.userTaskId() != null) {
            status.setSessionId(r.userTaskId());
        }
        if (r.status() == Status.COMPLETED) {
            applyProposal(status, r);
            return transition(cr, status, Phase.PROPOSAL_READY, proposalReadyMessage(), null);
        }
        return transition(cr, status, Phase.PENDING_PROPOSAL,
                "Waiting for Cruise Control to compute the optimization proposal", POLL);
    }

    private static String proposalReadyMessage() {
        return "Optimization proposal ready — annotate " + KafkaRebalance.REBALANCE_ANNOTATION
                + "=approve to execute";
    }

    private UpdateControl<KafkaRebalance> onProposalReady(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                         CruiseControlEndpoint ep, RebalanceParams params,
                                                         String annotation) {
        if (KafkaRebalance.APPROVE.equals(annotation)) {
            clearAnnotation(cr);
            CruiseControlResponse r = cc.rebalance(ep, params, false, null);
            status.setExecutionTaskId(r.userTaskId());
            if (r.status() == Status.ERROR) {
                return fail(cr, status, "Rebalance execution rejected: " + detail(r));
            }
            return transition(cr, status, Phase.REBALANCING, "Rebalance execution started", POLL);
        }
        if (KafkaRebalance.REFRESH.equals(annotation)) {
            clearAnnotation(cr);
            return resetToNew(cr, status);
        }
        if (KafkaRebalance.STOP.equals(annotation)) {
            clearAnnotation(cr);
            return transition(cr, status, Phase.STOPPED, "Proposal discarded before execution", null);
        }
        // Idle — wait for the approval annotation (which re-triggers reconcile). No reschedule.
        return terminal(cr, status);
    }

    private UpdateControl<KafkaRebalance> onRebalancing(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                       CruiseControlEndpoint ep, RebalanceParams params,
                                                       String annotation) {
        if (KafkaRebalance.STOP.equals(annotation)) {
            clearAnnotation(cr);
            cc.stopExecution(ep);
            return transition(cr, status, Phase.STOPPED, "Rebalance execution stopped by user", null);
        }
        CruiseControlResponse r = cc.rebalance(ep, params, false, status.getExecutionTaskId());
        if (r.status() == Status.ERROR) {
            return fail(cr, status, "Rebalance execution failed: " + detail(r));
        }
        if (r.status() == Status.IN_PROGRESS) {
            return transition(cr, status, Phase.REBALANCING,
                    "Cruise Control is computing the rebalance plan", POLL);
        }
        // COMPLETED — the plan was submitted; poll the executor until it goes idle.
        CruiseControlState state = cc.state(ep);
        if (state.executorIdle()) {
            return transition(cr, status, Phase.READY, "Rebalance complete", null);
        }
        return transition(cr, status, Phase.REBALANCING, "Rebalance in progress", POLL);
    }

    private UpdateControl<KafkaRebalance> onSettled(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                   String annotation) {
        if (KafkaRebalance.REFRESH.equals(annotation)) {
            clearAnnotation(cr);
            return resetToNew(cr, status);
        }
        return terminal(cr, status);
    }

    private UpdateControl<KafkaRebalance> onNotReady(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                    String annotation) {
        if (KafkaRebalance.REFRESH.equals(annotation)) {
            clearAnnotation(cr);
            return resetToNew(cr, status);
        }
        // Stay NOT_READY — re-triggered by a refresh annotation or a spec edit.
        return terminal(cr, status);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────

    /** @return an error message if the spec is invalid, else null. */
    static String validate(KafkaRebalanceSpec spec) {
        if (spec == null) return "spec is required";
        KafkaRebalanceMode mode = spec.getMode() == null ? KafkaRebalanceMode.FULL : spec.getMode();
        if ((mode == KafkaRebalanceMode.ADD_BROKERS || mode == KafkaRebalanceMode.REMOVE_BROKERS)
                && (spec.getBrokers() == null || spec.getBrokers().isEmpty())) {
            return "spec.brokers must list at least one broker ID for mode " + mode;
        }
        return null;
    }

    private void applyProposal(KafkaRebalanceStatus status, CruiseControlResponse r) {
        status.getOptimizationResult().clear();
        status.getOptimizationResult().putAll(r.summary());
        status.setDataToMoveMB(r.summary().get("dataToMoveMB"));
    }

    private static void resetProposalState(KafkaRebalanceStatus status) {
        status.setSessionId(null);
        status.setExecutionTaskId(null);
        status.getOptimizationResult().clear();
        status.setDataToMoveMB(null);
    }

    private UpdateControl<KafkaRebalance> resetToNew(KafkaRebalance cr, KafkaRebalanceStatus status) {
        resetProposalState(status);
        return transition(cr, status, Phase.NEW, "Regenerating the optimization proposal", FAST);
    }

    private UpdateControl<KafkaRebalance> fail(KafkaRebalance cr, KafkaRebalanceStatus status, String msg) {
        return transition(cr, status, Phase.NOT_READY, msg, null);
    }

    /** Sets phase + message, refreshes the Ready condition, and patches status. */
    private UpdateControl<KafkaRebalance> transition(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                     Phase phase, String message, Duration reschedule) {
        status.setPhase(phase);
        status.setMessage(message);
        setReadyCondition(cr, status);
        cr.setStatus(status);
        UpdateControl<KafkaRebalance> uc = UpdateControl.patchStatus(cr);
        return reschedule != null ? uc.rescheduleAfter(reschedule) : uc;
    }

    /** Patches status without changing the phase (terminal/idle pass). */
    private UpdateControl<KafkaRebalance> terminal(KafkaRebalance cr, KafkaRebalanceStatus status) {
        setReadyCondition(cr, status);
        cr.setStatus(status);
        return UpdateControl.patchStatus(cr);
    }

    private static void setReadyCondition(KafkaRebalance cr, KafkaRebalanceStatus status) {
        boolean ready = status.getPhase() == Phase.READY;
        Condition c = new Condition(
                "Ready",
                ready ? "True" : "False",
                status.getPhase().name(),
                status.getMessage() == null ? "" : status.getMessage(),
                Instant.now().toString(),
                cr.getMetadata().getGeneration());
        status.setConditions(List.of(c));
    }

    /** Removes the consumed approval annotation, in-memory and on the API server. */
    private void clearAnnotation(KafkaRebalance cr) {
        if (cr.getMetadata().getAnnotations() != null) {
            cr.getMetadata().getAnnotations().remove(KafkaRebalance.REBALANCE_ANNOTATION);
        }
        try {
            client.resources(KafkaRebalance.class)
                    .inNamespace(cr.getMetadata().getNamespace())
                    .withName(cr.getMetadata().getName())
                    .edit(r -> {
                        if (r.getMetadata().getAnnotations() != null) {
                            r.getMetadata().getAnnotations()
                                    .remove(KafkaRebalance.REBALANCE_ANNOTATION);
                        }
                        return r;
                    });
        } catch (Exception e) {
            LOG.warnf("Failed to clear %s annotation on %s/%s: %s",
                    KafkaRebalance.REBALANCE_ANNOTATION, cr.getMetadata().getNamespace(),
                    cr.getMetadata().getName(), e.getMessage());
        }
    }

    private static String detail(CruiseControlResponse r) {
        return r.summary() == null ? "(no detail)"
                : r.summary().getOrDefault("message", "(no detail)");
    }

    /**
     * True when a rejected proposal request is a transient "still warming up" condition —
     * Cruise Control has not yet collected enough metric windows / samples. Such a request
     * should be retried, not failed.
     */
    static boolean isWarmingUp(CruiseControlResponse r) {
        String m = detail(r);
        return m != null
                && (m.contains("NotEnoughValidWindows") || m.contains("NotEnoughSnapshots"));
    }
}
