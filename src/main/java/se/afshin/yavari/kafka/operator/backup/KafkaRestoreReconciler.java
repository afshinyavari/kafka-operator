package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackup;
import se.afshin.yavari.kafka.operator.crd.KafkaRestore;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreStatus;
import se.afshin.yavari.kafka.operator.crd.RestoreSourceSpec;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Reconciler for {@link KafkaRestore}. One-shot and idempotent: the restore Job is created
 * exactly once and, once {@code status.phase} is terminal, the reconciler does nothing.
 * Re-running a restore requires deleting and re-creating the CR.
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaRestoreReconciler implements Reconciler<KafkaRestore>, Cleaner<KafkaRestore> {

    private static final Logger LOG = Logger.getLogger(KafkaRestoreReconciler.class);

    @Inject KubernetesClient client;
    @Inject BackupPlacementGate placementGate;
    @Inject BackupEndpointResolver endpointResolver;
    @Inject RestoreConfigBuilder configBuilder;
    @Inject BackupConfigMapBuilder configMapBuilder;
    @Inject BackupWorkloadBuilder workloadBuilder;
    @Inject RestoreTargetChecker targetChecker;

    @Override
    public UpdateControl<KafkaRestore> reconcile(KafkaRestore cr, Context<KafkaRestore> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaRestore> reconcileInner(KafkaRestore cr) {
        String name = cr.getMetadata().getName();
        String ns = cr.getMetadata().getNamespace();

        KafkaRestoreStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaRestoreStatus();

        // Idempotency gate — a finished restore is never re-run.
        if (isTerminal(status.getPhase())) {
            return UpdateControl.noUpdate();
        }
        LOG.infof("Reconciling KafkaRestore %s/%s", ns, name);
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        BackupPlacementGate.Decision decision = placementGate.evaluate(cr.getSpec().getPlacement());
        if (decision == BackupPlacementGate.Decision.SKIP) {
            status.setPhase(KafkaRestoreStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + placementGate.localClusterId()
                    + "' is not the placement cluster for this restore");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
        if (decision == BackupPlacementGate.Decision.MISSING_PLACEMENT) {
            status.setPhase(KafkaRestoreStatus.Phase.FAILED);
            status.setMessage("spec.placement.clusterId is required when the operator runs multi-cluster");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
        if (!cr.getSpec().isConfirm()) {
            status.setPhase(KafkaRestoreStatus.Phase.FAILED);
            status.setMessage("restore is destructive — set spec.confirm=true to proceed");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        try {
            Job existing = client.batch().v1().jobs().inNamespace(ns).withName(name).get();
            if (existing == null) {
                return createRestoreJob(cr, status, name, ns);
            }
            if (BackupJobStatusReader.succeeded(existing)) {
                status.setPhase(KafkaRestoreStatus.Phase.SUCCEEDED);
                status.setMessage(null);
                status.setCompletionTime(existing.getStatus().getCompletionTime());
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr);
            }
            if (BackupJobStatusReader.failed(existing)) {
                status.setPhase(KafkaRestoreStatus.Phase.FAILED);
                status.setMessage("restore Job failed — inspect Job/" + name + " pod logs");
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr);
            }
            status.setPhase(KafkaRestoreStatus.Phase.RUNNING);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
        } catch (Exception e) {
            LOG.errorf("KafkaRestore %s/%s failed: %s", ns, name, e.getMessage());
            status.setPhase(KafkaRestoreStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
    }

    private UpdateControl<KafkaRestore> createRestoreJob(KafkaRestore cr, KafkaRestoreStatus status,
                                                        String name, String ns) {
        ResolvedBackupEndpoint endpoint =
                endpointResolver.resolve(cr.getSpec().getTargetClusterRef(), ns);
        status.setResolvedBootstrap(endpoint.bootstrap());

        BackupStorageSpec storage = resolveStorage(cr, ns);
        String backupId = resolveBackupId(cr);

        String violation = targetChecker.check(cr.getSpec().getTargetPolicy(), endpoint, ns,
                literalTargetTopics(cr.getSpec()));
        if (violation != null) {
            status.setPhase(KafkaRestoreStatus.Phase.FAILED);
            status.setMessage(violation);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        boolean importSchemas = resolveRestoreSchemas(cr, endpoint, storage);
        OwnerReference ownerRef = ownerRef(cr);

        String yaml = configBuilder.build(cr, endpoint, storage, backupId);
        ConfigMap cm = configMapBuilder.build(cr, BackupConfigMapBuilder.RESTORE_KEY, yaml,
                BackupLabels.labels("kafka-restore", name), ownerRef);
        client.configMaps().inNamespace(ns).resource(cm).serverSideApply();

        Job job = workloadBuilder.buildRestoreJob(cr, endpoint, storage, importSchemas, name, ownerRef);
        client.batch().v1().jobs().inNamespace(ns).resource(job).create();

        status.setPhase(KafkaRestoreStatus.Phase.RUNNING);
        status.setJobName(name);
        status.setStartTime(Instant.now().toString());
        status.setMessage(null);
        cr.setStatus(status);
        return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
    }

    @Override
    public DeleteControl cleanup(KafkaRestore cr, Context<KafkaRestore> ctx) {
        // Job + ConfigMap cascade via ownerReferences. Restored data in Kafka remains.
        LOG.infof("KafkaRestore %s/%s deleted", cr.getMetadata().getNamespace(),
                cr.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    static boolean isTerminal(KafkaRestoreStatus.Phase phase) {
        return phase == KafkaRestoreStatus.Phase.SUCCEEDED
                || phase == KafkaRestoreStatus.Phase.FAILED
                || phase == KafkaRestoreStatus.Phase.SKIPPED;
    }

    private BackupStorageSpec resolveStorage(KafkaRestore cr, String ns) {
        RestoreSourceSpec source = cr.getSpec().getSource();
        if (source.hasInlineStorage()) {
            return source.getStorage();
        }
        KafkaBackup backup = client.resources(KafkaBackup.class)
                .inNamespace(ns).withName(source.getKafkaBackupRef()).get();
        if (backup == null) {
            throw new IllegalStateException("source.kafkaBackupRef '" + source.getKafkaBackupRef()
                    + "' not found in namespace " + ns);
        }
        return backup.getSpec().getStorage();
    }

    private String resolveBackupId(KafkaRestore cr) {
        if (cr.getSpec().getBackupId() != null && !cr.getSpec().getBackupId().isBlank()) {
            return cr.getSpec().getBackupId();
        }
        if (cr.getSpec().getSource().hasBackupRef()) {
            return cr.getSpec().getSource().getKafkaBackupRef();
        }
        throw new IllegalStateException(
                "spec.backupId is required when source.storage is inline");
    }

    private boolean resolveRestoreSchemas(KafkaRestore cr, ResolvedBackupEndpoint endpoint,
                                          BackupStorageSpec storage) {
        Boolean explicit = cr.getSpec().getRestoreSchemas();
        boolean wanted = explicit != null ? explicit
                : endpoint.hasApicurio() && BackupStorageMounts.schemaExportSupported(storage);
        if (wanted && !endpoint.hasApicurio()) {
            throw new IllegalStateException(
                    "restoreSchemas is set but the target cluster has no Apicurio registry");
        }
        if (wanted && !BackupStorageMounts.schemaExportSupported(storage)) {
            throw new IllegalStateException(
                    "schema import is only supported for pvc/s3 storage in v1");
        }
        return wanted;
    }

    /** Literal (non-glob) target topic names to pre-flight check, after applying topicMapping. */
    static List<String> literalTargetTopics(KafkaRestoreSpec spec) {
        Set<String> out = new LinkedHashSet<>();
        if (spec.getTopics() != null && spec.getTopics().getInclude() != null) {
            for (String t : spec.getTopics().getInclude()) {
                if (t == null || t.contains("*")) {
                    continue;
                }
                out.add(spec.getTopicMapping() != null
                        ? spec.getTopicMapping().getOrDefault(t, t) : t);
            }
        }
        return new ArrayList<>(out);
    }

    static OwnerReference ownerRef(KafkaRestore cr) {
        return new OwnerReferenceBuilder()
                .withApiVersion(cr.getApiVersion())
                .withKind(cr.getKind())
                .withName(cr.getMetadata().getName())
                .withUid(cr.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();
    }
}
