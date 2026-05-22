package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
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
import se.afshin.yavari.kafka.operator.crd.KafkaBackupStatus;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Reconciler for {@link KafkaBackup}. Thin orchestrator (CLAUDE.md): placement gate →
 * resolve the cluster → render the kafka-backup config → apply a ConfigMap + a {@code CronJob}.
 * Kubernetes owns the schedule; this reconciler only re-renders on spec change and reflects
 * run results into status.
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaBackupReconciler implements Reconciler<KafkaBackup>, Cleaner<KafkaBackup> {

    private static final Logger LOG = Logger.getLogger(KafkaBackupReconciler.class);

    @Inject KubernetesClient client;
    @Inject BackupPlacementGate placementGate;
    @Inject BackupEndpointResolver endpointResolver;
    @Inject BackupConfigBuilder configBuilder;
    @Inject BackupConfigMapBuilder configMapBuilder;
    @Inject BackupWorkloadBuilder workloadBuilder;
    @Inject BackupJobStatusReader jobStatusReader;
    @Inject SecretRevisionTracker secretRevisionTracker;

    @Override
    public UpdateControl<KafkaBackup> reconcile(KafkaBackup cr, Context<KafkaBackup> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaBackup> reconcileInner(KafkaBackup cr) {
        String name = cr.getMetadata().getName();
        String ns = cr.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaBackup %s/%s", ns, name);

        KafkaBackupStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaBackupStatus();
        status.setPhase(KafkaBackupStatus.Phase.RECONCILING);
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        BackupPlacementGate.Decision decision = placementGate.evaluate(cr.getSpec().getPlacement());
        if (decision == BackupPlacementGate.Decision.SKIP) {
            status.setPhase(KafkaBackupStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + placementGate.localClusterId()
                    + "' is not the placement cluster for this backup");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }
        if (decision == BackupPlacementGate.Decision.MISSING_PLACEMENT) {
            status.setPhase(KafkaBackupStatus.Phase.FAILED);
            status.setMessage("spec.placement.clusterId is required when the operator runs multi-cluster");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        try {
            ResolvedBackupEndpoint endpoint =
                    endpointResolver.resolve(cr.getSpec().getClusterRef(), ns);
            status.setResolvedBootstrap(endpoint.bootstrap());

            boolean includeSchemas = resolveIncludeSchemas(cr, endpoint);
            if (includeSchemas) {
                if (!endpoint.hasApicurio()) {
                    throw new IllegalStateException(
                            "includeSchemas is set but the referenced cluster has no Apicurio registry");
                }
                if (!BackupStorageMounts.schemaExportSupported(cr.getSpec().getStorage())) {
                    throw new IllegalStateException(
                            "schema export is only supported for pvc/s3 storage in v1");
                }
            }
            verifyStorageSecret(cr.getSpec().getStorage(), ns);

            OwnerReference ownerRef = ownerRef(cr);
            String yaml = configBuilder.build(cr, endpoint);
            String secretRevisions = secretRevisionTracker.revisionsOf(secretRefs(cr, endpoint), ns);
            String configHash = ConfigHasher.sha256(yaml, secretRevisions);

            ConfigMap cm = configMapBuilder.build(cr, BackupConfigMapBuilder.BACKUP_KEY, yaml,
                    BackupLabels.labels("kafka-backup", name), ownerRef);
            client.configMaps().inNamespace(ns).resource(cm).serverSideApply();

            CronJob cronJob = workloadBuilder.buildBackupCronJob(
                    cr, endpoint, configHash, includeSchemas, ownerRef);
            client.batch().v1().cronjobs().inNamespace(ns).resource(cronJob).serverSideApply();

            status.setCronJobName(name);
            status.setSchedule(cr.getSpec().getSchedule());
            jobStatusReader.populateBackup(status, name, ns);
            status.setPhase(cr.getSpec().isSuspend()
                    ? KafkaBackupStatus.Phase.SUSPENDED : KafkaBackupStatus.Phase.SCHEDULED);
            status.setMessage(null);
        } catch (Exception e) {
            LOG.errorf("KafkaBackup %s/%s failed: %s", ns, name, e.getMessage());
            status.setPhase(KafkaBackupStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        cr.setStatus(status);
        return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(60));
    }

    @Override
    public DeleteControl cleanup(KafkaBackup cr, Context<KafkaBackup> ctx) {
        // ConfigMap + CronJob (and its Jobs/Pods) cascade via ownerReferences. Backups
        // already written to object storage are intentionally left untouched.
        LOG.infof("KafkaBackup %s/%s deleted", cr.getMetadata().getNamespace(),
                cr.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }

    /** Auto-enables schema export when the cluster has Apicurio, unless the spec is explicit. */
    static boolean resolveIncludeSchemas(KafkaBackup cr, ResolvedBackupEndpoint endpoint) {
        Boolean explicit = cr.getSpec().getIncludeSchemas();
        return explicit != null ? explicit : endpoint.hasApicurio();
    }

    private void verifyStorageSecret(BackupStorageSpec storage, String namespace) {
        String secret = storage.credentialsSecretRef();
        if (secret != null && client.secrets().inNamespace(namespace).withName(secret).get() == null) {
            throw new IllegalStateException(
                    "storage credentials Secret '" + secret + "' not found in namespace " + namespace);
        }
    }

    static List<String> secretRefs(KafkaBackup cr, ResolvedBackupEndpoint endpoint) {
        List<String> refs = new ArrayList<>();
        if (endpoint.tlsSecretRef() != null) {
            refs.add(endpoint.tlsSecretRef());
        }
        String storageSecret = cr.getSpec().getStorage().credentialsSecretRef();
        if (storageSecret != null) {
            refs.add(storageSecret);
        }
        if (cr.getSpec().getSchemaRegistryAuthSecretRef() != null) {
            refs.add(cr.getSpec().getSchemaRegistryAuthSecretRef());
        }
        return refs;
    }

    static OwnerReference ownerRef(KafkaBackup cr) {
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
