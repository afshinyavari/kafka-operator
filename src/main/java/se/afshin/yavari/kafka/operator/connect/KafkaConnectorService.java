package se.afshin.yavari.kafka.operator.connect;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaConnector;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorConfigFromSource;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectorStatus;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Business logic for KafkaConnector reconciliation. Takes a {@link ConnectRestClient},
 * does drift detection + push/pull, and returns the updated status. Reconciler stays a
 * thin orchestrator (CLAUDE.md rule).
 */
@ApplicationScoped
public class KafkaConnectorService {

    private static final Logger LOG = Logger.getLogger(KafkaConnectorService.class);

    @Inject KubernetesClient client;
    @Inject ConnectorConfigRenderer renderer;
    @Inject ConnectorDriftDetector driftDetector;
    @Inject ConnectStatusMapper statusMapper;
    @Inject ConnectRestClient restClient;
    @Inject SecretRevisionTracker secretRevisionTracker;

    /** Returned by {@link #reconcile} for the reconciler to act on. */
    public record Outcome(KafkaConnectorStatus status, boolean wroteConfig) {}

    public Outcome reconcile(KafkaConnector cr, ConnectEndpoint endpoint) {
        KafkaConnectorSpec spec = cr.getSpec();
        String namespace = cr.getMetadata().getNamespace();
        String name = cr.resolvedConnectorName();
        KafkaConnectorStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaConnectorStatus();
        status.setObservedGeneration(cr.getMetadata().getGeneration());
        status.setLastReconcileTime(Instant.now().toString());

        // 1. Resolve configFrom secret (if any).
        Map<String, String> resolvedSecretValues = resolveConfigFrom(spec.getConfigFrom(), namespace);

        // 2. Render desired config.
        Map<String, String> desired = renderer.render(cr, resolvedSecretValues);

        // 3. Compute desired hash (canonical = sorted JSON-ish) + Secret revisions.
        String desiredHash = hash(desired, spec.getConfigFrom(), namespace);

        // 4. Drift check + write.
        boolean wrote = false;
        try {
            Optional<Map<String, String>> actualOpt = restClient.getConfig(endpoint.baseUrl(), name);
            if (actualOpt.isEmpty()) {
                restClient.create(endpoint.baseUrl(), name, desired);
                LOG.infof("Created connector %s", name);
                wrote = true;
            } else if (!desiredHash.equals(status.getObservedConfigHash())
                    || driftDetector.drifted(desired, actualOpt.get())) {
                restClient.putConfig(endpoint.baseUrl(), name, desired);
                LOG.infof("Updated connector %s (hash %s)", name, desiredHash);
                wrote = true;
            }
            if (wrote) {
                status.setObservedConfigHash(desiredHash);
            }
        } catch (ConnectRestException e) {
            return handleRestException(status, e, "config sync");
        }

        // 5. Sync runtime state.
        try {
            syncState(spec.getState(), status.getConnectorState(), endpoint.baseUrl(), name);
        } catch (ConnectRestException e) {
            return handleRestException(status, e, "state sync");
        }

        // 6. Read status.
        try {
            Optional<JsonNode> root = restClient.status(endpoint.baseUrl(), name);
            if (root.isEmpty()) {
                status.setPhase(KafkaConnectorStatus.Phase.Reconciling);
                status.setMessage("Connect REST reports connector not yet visible");
                return new Outcome(status, wrote);
            }
            statusMapper.map(root.get(), status);
        } catch (ConnectRestException e) {
            return handleRestException(status, e, "status read");
        }

        // 7. Compute phase.
        computePhase(spec, status);
        status.setMessage(null);
        return new Outcome(status, wrote);
    }

    /** Cleanup path — best-effort REST delete. */
    public void delete(String baseUrl, String connectorName) {
        try {
            restClient.delete(baseUrl, connectorName);
        } catch (ConnectRestException e) {
            LOG.warnf("DELETE /connectors/%s failed (%d): %s — releasing finalizer anyway",
                    connectorName, e.httpStatus(), e.getMessage());
        }
    }

    private Map<String, String> resolveConfigFrom(KafkaConnectorConfigFromSource cf, String namespace) {
        if (cf == null || cf.getSecretRef() == null) return Map.of();
        Secret s = client.secrets().inNamespace(namespace).withName(cf.getSecretRef()).get();
        if (s == null) {
            throw new IllegalStateException("spec.configFrom.secretRef='" + cf.getSecretRef()
                    + "' not found in namespace " + namespace);
        }
        Map<String, String> out = new LinkedHashMap<>();
        String prefix = cf.getPrefix() == null ? "" : cf.getPrefix();
        if (s.getData() != null) {
            for (var e : s.getData().entrySet()) {
                out.put(prefix + e.getKey(),
                        new String(Base64.getDecoder().decode(e.getValue()), StandardCharsets.UTF_8));
            }
        }
        if (s.getStringData() != null) {
            for (var e : s.getStringData().entrySet()) {
                out.put(prefix + e.getKey(), e.getValue());
            }
        }
        return out;
    }

    private String hash(Map<String, String> desired, KafkaConnectorConfigFromSource cf, String namespace) {
        // Canonicalise: sorted key=value lines.
        TreeMap<String, String> sorted = new TreeMap<>(desired);
        StringBuilder sb = new StringBuilder();
        sorted.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        String secretRev = cf != null && cf.getSecretRef() != null
                ? secretRevisionTracker.revisionsOf(List.of(cf.getSecretRef()), namespace)
                : "";
        return ConfigHasher.sha256(sb.toString(), secretRev);
    }

    private void syncState(KafkaConnectorSpec.State desired, String actual, String baseUrl, String name) {
        if (desired == null) desired = KafkaConnectorSpec.State.running;
        // Compare actual REST connector.state against desired enum. Connect states are
        // upper-case: RUNNING / PAUSED / STOPPED / UNASSIGNED / FAILED.
        switch (desired) {
            case running -> {
                if (actual != null && (actual.equals("PAUSED") || actual.equals("STOPPED"))) {
                    restClient.resume(baseUrl, name);
                }
            }
            case paused -> {
                if (actual == null || !actual.equals("PAUSED")) {
                    restClient.pause(baseUrl, name);
                }
            }
            case stopped -> {
                if (actual == null || !actual.equals("STOPPED")) {
                    restClient.stop(baseUrl, name);
                }
            }
        }
    }

    private void computePhase(KafkaConnectorSpec spec, KafkaConnectorStatus status) {
        String connectorState = status.getConnectorState();
        boolean anyTaskFailed = status.getTasks() != null
                && status.getTasks().stream().anyMatch(t -> "FAILED".equals(t.getState()));
        if ("FAILED".equals(connectorState) || anyTaskFailed) {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            return;
        }
        if (spec.getState() == KafkaConnectorSpec.State.paused && "PAUSED".equals(connectorState)) {
            status.setPhase(KafkaConnectorStatus.Phase.Paused);
            return;
        }
        if (spec.getState() == KafkaConnectorSpec.State.stopped && "STOPPED".equals(connectorState)) {
            status.setPhase(KafkaConnectorStatus.Phase.Stopped);
            return;
        }
        if ("RUNNING".equals(connectorState)
                && status.getTasksTotal() != null
                && status.getTasksTotal() == spec.getTasksMax()
                && status.getTasks() != null
                && status.getTasks().stream().allMatch(t -> "RUNNING".equals(t.getState()))) {
            status.setPhase(KafkaConnectorStatus.Phase.Ready);
            return;
        }
        status.setPhase(KafkaConnectorStatus.Phase.Reconciling);
    }

    private static Outcome handleRestException(KafkaConnectorStatus status,
                                                ConnectRestException e, String op) {
        int sc = e.httpStatus();
        if (sc == 409) {
            status.setPhase(KafkaConnectorStatus.Phase.Reconciling);
            status.setMessage(op + ": rebalance in progress (HTTP 409)");
        } else if (sc == 400) {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            status.setMessage(op + ": invalid configuration (HTTP 400): " + e.getMessage());
        } else {
            status.setPhase(KafkaConnectorStatus.Phase.Failed);
            status.setMessage(op + ": " + e.getMessage());
        }
        return new Outcome(status, false);
    }
}
