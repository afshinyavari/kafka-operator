package se.afshin.yavari.kafka.operator.connect;

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
import se.afshin.yavari.kafka.operator.crd.KafkaConnector;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorStatus;
import se.afshin.yavari.kafka.operator.infra.ConditionUtil;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reconciler for the {@link KafkaConnector} CRD. Pushes desired config to the parent
 * KafkaConnect's REST API and reconciles drift back to spec.
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaConnectorReconciler implements Reconciler<KafkaConnector>, Cleaner<KafkaConnector> {

    private static final Logger LOG = Logger.getLogger(KafkaConnectorReconciler.class);

    @Inject ConnectEndpointResolver endpointResolver;
    @Inject KafkaConnectorService service;
    @Inject ConnectRestClient restClient;

    @ConfigProperty(name = "kafka.connector.poll.interval.seconds", defaultValue = "15")
    long pollIntervalSeconds;

    /** Tracks per-CR auto-restart attempts so we don't retry forever. Reset on operator
     *  restart by virtue of being in-memory. Keyed by namespace/name. */
    final Map<String, Integer> autoRestartAttempts = new ConcurrentHashMap<>();

    @Override
    public UpdateControl<KafkaConnector> reconcile(KafkaConnector cr, Context<KafkaConnector> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaConnector> reconcileInner(KafkaConnector cr) {
        String namespace = cr.getMetadata().getNamespace();
        String connectorName = cr.resolvedConnectorName();
        KafkaConnectorStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaConnectorStatus();
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        if (cr.getSpec() == null || cr.getSpec().getConnectClusterRef() == null) {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            status.setMessage("spec.connectClusterRef is required");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        ConnectEndpoint endpoint = endpointResolver.resolve(
                cr.getSpec().getConnectClusterRef().getName(), namespace);

        if (endpoint.baseUrl() == null) {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            status.setMessage(endpoint.message());
            applyConditions(status);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
        }

        if (!endpoint.ready()) {
            status.setPhase(KafkaConnectorStatus.Phase.Reconciling);
            status.setMessage(endpoint.message());
            applyConditions(status);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
        }

        try {
            KafkaConnectorService.Outcome outcome = service.reconcile(cr, endpoint);
            status = outcome.status();
        } catch (IllegalStateException e) {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            status.setMessage(e.getMessage());
            applyConditions(status);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(30));
        }

        // Auto-restart.
        if (cr.getSpec().getAutoRestart() != null && cr.getSpec().getAutoRestart().isEnabled()
                && status.getPhase() == KafkaConnectorStatus.Phase.Failed) {
            String key = namespace + "/" + cr.getMetadata().getName();
            int attempts = autoRestartAttempts.getOrDefault(key, 0);
            int max = cr.getSpec().getAutoRestart().getMaxRetries();
            if (attempts < max) {
                try {
                    restClient.restart(endpoint.baseUrl(), connectorName, true, true);
                    autoRestartAttempts.put(key, attempts + 1);
                    LOG.infof("Auto-restart attempt %d/%d for connector %s", attempts + 1, max, connectorName);
                    status.setMessage("Auto-restart attempt " + (attempts + 1) + "/" + max + " in flight");
                } catch (ConnectRestException e) {
                    LOG.warnf("Auto-restart failed for %s: %s", connectorName, e.getMessage());
                }
                applyConditions(status);
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(30));
            }
        } else if (status.getPhase() == KafkaConnectorStatus.Phase.Ready) {
            // Clear retry budget once a connector recovers.
            autoRestartAttempts.remove(namespace + "/" + cr.getMetadata().getName());
        }

        applyConditions(status);
        cr.setStatus(status);

        Duration next = switch (status.getPhase()) {
            case Reconciling -> Duration.ofSeconds(5);
            case Failed -> Duration.ofSeconds(60);
            default -> Duration.ofSeconds(pollIntervalSeconds);
        };
        return UpdateControl.patchStatus(cr).rescheduleAfter(next);
    }

    @Override
    public DeleteControl cleanup(KafkaConnector cr, Context<KafkaConnector> ctx) {
        String namespace = cr.getMetadata().getNamespace();
        String connectorName = cr.resolvedConnectorName();
        if (cr.getSpec() != null && cr.getSpec().getConnectClusterRef() != null) {
            ConnectEndpoint endpoint = endpointResolver.resolve(
                    cr.getSpec().getConnectClusterRef().getName(), namespace);
            if (endpoint.baseUrl() != null && endpoint.ready()) {
                service.delete(endpoint.baseUrl(), connectorName);
            } else {
                LOG.infof("Parent KafkaConnect for %s/%s unavailable — releasing finalizer without REST call",
                        namespace, cr.getMetadata().getName());
            }
        }
        autoRestartAttempts.remove(namespace + "/" + cr.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    private static void applyConditions(KafkaConnectorStatus status) {
        Long observedGen = status.getObservedGeneration();
        String availableStatus = status.getPhase() == KafkaConnectorStatus.Phase.Ready
                ? ConditionUtil.TRUE : ConditionUtil.FALSE;
        String progressingStatus = status.getPhase() == KafkaConnectorStatus.Phase.Reconciling
                ? ConditionUtil.TRUE : ConditionUtil.FALSE;
        String degradedStatus = status.getPhase() == KafkaConnectorStatus.Phase.Failed
                ? ConditionUtil.TRUE : ConditionUtil.FALSE;
        var conditions = status.getConditions();
        conditions = ConditionUtil.set(conditions, ConditionUtil.AVAILABLE, availableStatus,
                status.getPhase().name(), status.getMessage(), observedGen);
        conditions = ConditionUtil.set(conditions, ConditionUtil.PROGRESSING, progressingStatus,
                status.getPhase().name(), status.getMessage(), observedGen);
        conditions = ConditionUtil.set(conditions, ConditionUtil.DEGRADED, degradedStatus,
                status.getPhase().name(), status.getMessage(), observedGen);
        status.setConditions(conditions);
    }
}
