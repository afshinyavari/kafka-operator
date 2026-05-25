package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.OwnerReference;
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
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidation;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidationStatus;
import se.afshin.yavari.kafka.operator.crd.RestoreSourceSpec;
import se.afshin.yavari.kafka.operator.infra.OwnerReferences;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Reconciler for {@link KafkaBackupValidation}. One-shot and idempotent — builds a single
 * {@code Job} that runs {@code kafka-backup validate}; the terminal phase reflects whether
 * the stored backup is restorable.
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaBackupValidationReconciler
        implements Reconciler<KafkaBackupValidation>, Cleaner<KafkaBackupValidation> {

    private static final Logger LOG = Logger.getLogger(KafkaBackupValidationReconciler.class);

    @Inject KubernetesClient client;
    @Inject BackupPlacementGate placementGate;
    @Inject ValidationConfigBuilder configBuilder;
    @Inject BackupWorkloadBuilder workloadBuilder;

    @Override
    public UpdateControl<KafkaBackupValidation> reconcile(KafkaBackupValidation cr,
                                                          Context<KafkaBackupValidation> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaBackupValidation> reconcileInner(KafkaBackupValidation cr) {
        String name = cr.getMetadata().getName();
        String ns = cr.getMetadata().getNamespace();

        KafkaBackupValidationStatus status = cr.getStatus() != null
                ? cr.getStatus() : new KafkaBackupValidationStatus();

        if (isTerminal(status.getPhase())) {
            return UpdateControl.noUpdate();
        }
        LOG.infof("Reconciling KafkaBackupValidation %s/%s", ns, name);
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        BackupPlacementGate.Decision decision = placementGate.evaluate(cr.getSpec().getPlacement());
        if (decision == BackupPlacementGate.Decision.SKIP) {
            status.setPhase(KafkaBackupValidationStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + placementGate.localClusterId()
                    + "' is not the placement cluster for this validation");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
        if (decision == BackupPlacementGate.Decision.MISSING_PLACEMENT) {
            status.setPhase(KafkaBackupValidationStatus.Phase.FAILED);
            status.setMessage("spec.placement.clusterId is required when the operator runs multi-cluster");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        try {
            Job existing = client.batch().v1().jobs().inNamespace(ns).withName(name).get();
            if (existing == null) {
                BackupStorageSpec storage = resolveStorage(cr, ns);
                String backupId = resolveBackupId(cr);
                List<String> args = configBuilder.validateArgs(cr, storage, backupId);
                Job job = workloadBuilder.buildValidationJob(cr, storage, args, name, ownerRef(cr));
                client.batch().v1().jobs().inNamespace(ns).resource(job).create();
                status.setPhase(KafkaBackupValidationStatus.Phase.RUNNING);
                status.setJobName(name);
                status.setStartTime(Instant.now().toString());
                status.setMessage(null);
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
            }
            if (BackupJobStatusReader.succeeded(existing)) {
                status.setPhase(KafkaBackupValidationStatus.Phase.VALID);
                status.setMessage(null);
                status.setCompletionTime(existing.getStatus().getCompletionTime());
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr);
            }
            if (BackupJobStatusReader.failed(existing)) {
                status.setPhase(KafkaBackupValidationStatus.Phase.INVALID);
                status.setMessage("validation reported the backup is invalid or unreadable — "
                        + "inspect Job/" + name + " pod logs");
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr);
            }
            status.setPhase(KafkaBackupValidationStatus.Phase.RUNNING);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
        } catch (Exception e) {
            LOG.errorf("KafkaBackupValidation %s/%s failed: %s", ns, name, e.getMessage());
            status.setPhase(KafkaBackupValidationStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
    }

    @Override
    public DeleteControl cleanup(KafkaBackupValidation cr, Context<KafkaBackupValidation> ctx) {
        LOG.infof("KafkaBackupValidation %s/%s deleted", cr.getMetadata().getNamespace(),
                cr.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    static boolean isTerminal(KafkaBackupValidationStatus.Phase phase) {
        return phase == KafkaBackupValidationStatus.Phase.VALID
                || phase == KafkaBackupValidationStatus.Phase.INVALID
                || phase == KafkaBackupValidationStatus.Phase.FAILED
                || phase == KafkaBackupValidationStatus.Phase.SKIPPED;
    }

    private BackupStorageSpec resolveStorage(KafkaBackupValidation cr, String ns) {
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

    private String resolveBackupId(KafkaBackupValidation cr) {
        if (cr.getSpec().getBackupId() != null && !cr.getSpec().getBackupId().isBlank()) {
            return cr.getSpec().getBackupId();
        }
        if (cr.getSpec().getSource().hasBackupRef()) {
            return cr.getSpec().getSource().getKafkaBackupRef();
        }
        throw new IllegalStateException("spec.backupId is required when source.storage is inline");
    }

    static OwnerReference ownerRef(KafkaBackupValidation cr) {
        return OwnerReferences.of(cr);
    }
}
