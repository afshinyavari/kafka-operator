package se.afshin.yavari.kafka.operator.rebalance;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.CruiseControlStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalance;
import se.afshin.yavari.kafka.operator.crd.KafkaRebalanceStatus;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;
import se.afshin.yavari.kafka.operator.rebalance.CruiseControlClient.CruiseControlEndpoint;
import se.afshin.yavari.kafka.operator.topic.TopicReconcileLeader;

import java.time.Instant;

/**
 * Thin reconciler for {@link KafkaRebalance}: resolves the target {@link KafkaCluster},
 * applies the primary-cluster gate, waits for Cruise Control to be READY, then hands the
 * whole transition to {@link RebalanceStateMachine}.
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaRebalanceReconciler implements Reconciler<KafkaRebalance>, Cleaner<KafkaRebalance> {

    private static final Logger LOG = Logger.getLogger(KafkaRebalanceReconciler.class);

    @Inject KubernetesClient client;
    @Inject RebalanceStateMachine stateMachine;
    @Inject CruiseControlEndpointResolver endpointResolver;
    @Inject CruiseControlClient ccClient;
    @Inject TopicReconcileLeader leader;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public UpdateControl<KafkaRebalance> reconcile(KafkaRebalance cr, Context<KafkaRebalance> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaRebalance> reconcileInner(KafkaRebalance cr) {
        String ns = cr.getMetadata().getNamespace();
        String name = cr.getMetadata().getName();
        String clusterRef = cr.getSpec().getClusterRef();
        LOG.infof("Reconciling KafkaRebalance %s/%s (cluster=%s)", ns, name, clusterRef);

        KafkaRebalanceStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaRebalanceStatus();

        KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                .withName(clusterRef).get();
        if (cluster == null) {
            return notReady(cr, status,
                    "Referenced KafkaCluster '" + clusterRef + "' not found in namespace " + ns);
        }

        // Primary-cluster gate — Cruise Control runs only on spec.clusters[0].
        String primary = leader.currentLeaderId(cluster);
        if (primary != null && !primary.equals(localClusterId)) {
            return notReady(cr, status, "Cruise Control runs on the primary cluster '" + primary
                    + "' — apply this KafkaRebalance there");
        }

        if (cluster.getSpec().getCruiseControl() == null) {
            return notReady(cr, status, "KafkaCluster '" + clusterRef
                    + "' has no spec.cruiseControl — enable Cruise Control first");
        }

        // Wait for Cruise Control to be READY before driving the state machine, so a
        // still-starting Cruise Control doesn't push the CR into terminal NOT_READY.
        CruiseControlStatus ccStatus = cluster.getStatus() != null
                ? cluster.getStatus().getCruiseControl() : null;
        if (ccStatus == null || ccStatus.getPhase() != CruiseControlStatus.Phase.READY) {
            return waiting(cr, status,
                    "Waiting for Cruise Control to be READY on KafkaCluster '" + clusterRef + "'");
        }

        CruiseControlEndpoint endpoint;
        try {
            endpoint = endpointResolver.resolve(cluster, ns);
        } catch (RuntimeException e) {
            return notReady(cr, status, e.getMessage());
        }

        return stateMachine.advance(cr, endpoint);
    }

    @Override
    public DeleteControl cleanup(KafkaRebalance cr, Context<KafkaRebalance> ctx) {
        // Best-effort: cancel an in-flight rebalance when the CR is deleted. Never blocks
        // deletion — the finalizer is always released.
        KafkaRebalanceStatus status = cr.getStatus();
        if (status != null && status.getPhase() == KafkaRebalanceStatus.Phase.REBALANCING) {
            String ns = cr.getMetadata().getNamespace();
            try {
                KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                        .withName(cr.getSpec().getClusterRef()).get();
                if (cluster != null && cluster.getSpec().getCruiseControl() != null) {
                    ccClient.stopExecution(endpointResolver.resolve(cluster, ns));
                    LOG.infof("KafkaRebalance %s/%s deleted mid-execution — asked Cruise Control to stop",
                            ns, cr.getMetadata().getName());
                }
            } catch (RuntimeException e) {
                LOG.warnf("KafkaRebalance %s/%s cleanup — could not stop execution: %s",
                        ns, cr.getMetadata().getName(), e.getMessage());
            }
        }
        return DeleteControl.defaultDelete();
    }

    private UpdateControl<KafkaRebalance> notReady(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                  String message) {
        status.setPhase(KafkaRebalanceStatus.Phase.NOT_READY);
        status.setMessage(message);
        status.setObservedGeneration(cr.getMetadata().getGeneration());
        status.setLastReconcileTime(Instant.now().toString());
        cr.setStatus(status);
        return UpdateControl.patchStatus(cr);
    }

    private UpdateControl<KafkaRebalance> waiting(KafkaRebalance cr, KafkaRebalanceStatus status,
                                                 String message) {
        if (status.getPhase() == null) {
            status.setPhase(KafkaRebalanceStatus.Phase.NEW);
        }
        status.setMessage(message);
        status.setLastReconcileTime(Instant.now().toString());
        cr.setStatus(status);
        return UpdateControl.patchStatus(cr).rescheduleAfter(RebalanceStateMachine.POLL);
    }
}
