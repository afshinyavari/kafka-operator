package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.CronJobBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackup;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidation;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupValidationSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaRestore;
import se.afshin.yavari.kafka.operator.crd.KafkaRestoreSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaUIResourceRequirements;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the Kubernetes workloads that run the kafka-backup tool: a {@code CronJob} for a
 * {@link KafkaBackup}, and one-shot {@code Job}s for {@link KafkaRestore} and
 * {@link KafkaBackupValidation}. All workloads run direct against the brokers; restartPolicy
 * is {@code Never} so a failed run surfaces rather than crash-looping silently.
 */
@ApplicationScoped
public class BackupWorkloadBuilder {

    public static final String CONFIG_HASH_ANNOTATION = "kafka.yavari.afshin.se/config-hash";
    static final String MAIN_CONTAINER = "kafka-backup";
    private static final String CONFIG_VOLUME = "config";
    private static final String TLS_VOLUME = "broker-tls";

    @Inject BackupStorageMounts storageMounts;
    @Inject SchemaBackupStep schemaBackupStep;

    /** CronJob for a recurring {@link KafkaBackup}. */
    public CronJob buildBackupCronJob(KafkaBackup cr, ResolvedBackupEndpoint endpoint,
                                      String configHash, boolean includeSchemas,
                                      OwnerReference ownerRef) {
        KafkaBackupSpec spec = cr.getSpec();
        String name = cr.getMetadata().getName();
        Map<String, String> labels = BackupLabels.labels("kafka-backup", name);
        ResourceRequirements res = resources(spec.getResources());
        BackupStorageMounts.StorageWiring wiring = storageMounts.forBackupTool(spec.getStorage());

        List<Volume> volumes = new ArrayList<>();
        volumes.add(configVolume(name));
        if (endpoint.hasTls()) {
            volumes.add(tlsVolume(endpoint.tlsSecretRef()));
        }
        volumes.addAll(wiring.volumes());

        List<Container> containers = new ArrayList<>();
        containers.add(mainContainer(spec.getImage(), spec.getImagePullPolicy(),
                List.of("backup", "--config",
                        BackupPaths.CONFIG_DIR + "/" + BackupConfigMapBuilder.BACKUP_KEY),
                BackupConfigMapBuilder.BACKUP_KEY, endpoint.hasTls(), wiring, res));
        if (includeSchemas) {
            containers.add(schemaBackupStep.exportSidecar(
                    spec.getImage(), spec.getImagePullPolicy(), endpoint.apicurioUrl(),
                    BackupStorageMounts.schemaLocation(spec.getStorage()),
                    spec.getSchemaRegistryAuthSecretRef(),
                    storageMounts.schemaToolEnv(spec.getStorage()), wiring.mounts(), res));
        }

        Map<String, String> podAnnotations = (configHash != null && !configHash.isBlank())
                ? Map.of(CONFIG_HASH_ANNOTATION, configHash)
                : Map.of();

        return new CronJobBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(cr.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withSchedule(spec.getSchedule())
                    .withSuspend(spec.isSuspend())
                    .withConcurrencyPolicy(spec.getConcurrencyPolicy())
                    .withStartingDeadlineSeconds(spec.getStartingDeadlineSeconds())
                    .withSuccessfulJobsHistoryLimit(spec.getSuccessfulJobsHistoryLimit())
                    .withFailedJobsHistoryLimit(spec.getFailedJobsHistoryLimit())
                    .withNewJobTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withActiveDeadlineSeconds(spec.getActiveDeadlineSeconds())
                            .withBackoffLimit(2)
                            .withNewTemplate()
                                .withNewMetadata()
                                    .withLabels(labels)
                                    .withAnnotations(podAnnotations)
                                .endMetadata()
                                .withNewSpec()
                                    .withRestartPolicy("Never")
                                    .withContainers(containers)
                                    .withVolumes(volumes)
                                    .withSecurityContext(SecurityContextDefaults.podDefaults())
                                .endSpec()
                            .endTemplate()
                        .endSpec()
                    .endJobTemplate()
                .endSpec()
                .build();
    }

    /** One-shot Job for a {@link KafkaRestore}. {@code backoffLimit=0} — a restore must
     *  never silently retry and double-write records. */
    public Job buildRestoreJob(KafkaRestore cr, ResolvedBackupEndpoint endpoint,
                               BackupStorageSpec storage, boolean importSchemas,
                               String jobName, OwnerReference ownerRef) {
        KafkaRestoreSpec spec = cr.getSpec();
        Map<String, String> labels = BackupLabels.labels("kafka-restore", cr.getMetadata().getName());
        ResourceRequirements res = resources(spec.getResources());
        BackupStorageMounts.StorageWiring wiring = storageMounts.forBackupTool(storage);

        List<Volume> volumes = new ArrayList<>();
        volumes.add(configVolume(cr.getMetadata().getName()));
        if (endpoint.hasTls()) {
            volumes.add(tlsVolume(endpoint.tlsSecretRef()));
        }
        volumes.addAll(wiring.volumes());

        Container main = mainContainer(spec.getImage(), spec.getImagePullPolicy(),
                List.of("restore", "--config",
                        BackupPaths.CONFIG_DIR + "/" + BackupConfigMapBuilder.RESTORE_KEY),
                BackupConfigMapBuilder.RESTORE_KEY, endpoint.hasTls(), wiring, res);

        List<Container> initContainers = new ArrayList<>();
        if (importSchemas) {
            initContainers.add(schemaBackupStep.importInitContainer(
                    spec.getImage(), spec.getImagePullPolicy(), endpoint.apicurioUrl(),
                    BackupStorageMounts.schemaLocation(storage),
                    spec.getSchemaRegistryAuthSecretRef(),
                    storageMounts.schemaToolEnv(storage), wiring.mounts(), res));
        }

        return new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                    .withNamespace(cr.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withActiveDeadlineSeconds(spec.getActiveDeadlineSeconds())
                    .withBackoffLimit(0)
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                            .withInitContainers(initContainers)
                            .withContainers(main)
                            .withVolumes(volumes)
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    /** One-shot Job for a {@link KafkaBackupValidation}. */
    public Job buildValidationJob(KafkaBackupValidation cr, BackupStorageSpec storage,
                                  List<String> validateArgs, String jobName,
                                  OwnerReference ownerRef) {
        KafkaBackupValidationSpec spec = cr.getSpec();
        Map<String, String> labels =
                BackupLabels.labels("kafka-backup-validation", cr.getMetadata().getName());
        ResourceRequirements res = resources(spec.getResources());
        BackupStorageMounts.StorageWiring wiring = storageMounts.forBackupTool(storage);

        Container main = mainContainer(spec.getImage(), spec.getImagePullPolicy(),
                validateArgs, null, false, wiring, res);

        return new JobBuilder()
                .withNewMetadata()
                    .withName(jobName)
                    .withNamespace(cr.getMetadata().getNamespace())
                    .withLabels(labels)
                    .withOwnerReferences(ownerRef)
                .endMetadata()
                .withNewSpec()
                    .withActiveDeadlineSeconds(spec.getActiveDeadlineSeconds())
                    .withBackoffLimit(1)
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withRestartPolicy("Never")
                            .withContainers(main)
                            .withVolumes(wiring.volumes())
                            .withSecurityContext(SecurityContextDefaults.podDefaults())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    /** The kafka-backup container. {@code configKey} null = no config file (validation). */
    private Container mainContainer(String image, String pullPolicy, List<String> args,
                                    String configKey, boolean tls,
                                    BackupStorageMounts.StorageWiring wiring,
                                    ResourceRequirements res) {
        List<VolumeMount> mounts = new ArrayList<>();
        if (configKey != null) {
            mounts.add(new VolumeMountBuilder()
                    .withName(CONFIG_VOLUME)
                    .withMountPath(BackupPaths.CONFIG_DIR + "/" + configKey)
                    .withSubPath(configKey)
                    .withReadOnly(true)
                    .build());
        }
        if (tls) {
            mounts.add(new VolumeMountBuilder()
                    .withName(TLS_VOLUME)
                    .withMountPath(BackupPaths.TLS_DIR)
                    .withReadOnly(true)
                    .build());
        }
        mounts.addAll(wiring.mounts());
        return new ContainerBuilder()
                .withName(MAIN_CONTAINER)
                .withImage(image)
                .withImagePullPolicy(pullPolicy)
                .withArgs(args)
                .withEnv(wiring.env())
                .withVolumeMounts(mounts)
                .withResources(res)
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();
    }

    private static Volume configVolume(String configMapName) {
        return new VolumeBuilder()
                .withName(CONFIG_VOLUME)
                .withNewConfigMap().withName(configMapName).endConfigMap()
                .build();
    }

    private static Volume tlsVolume(String secretName) {
        return new VolumeBuilder()
                .withName(TLS_VOLUME)
                .withNewSecret().withSecretName(secretName).endSecret()
                .build();
    }

    static ResourceRequirements resources(KafkaUIResourceRequirements r) {
        if (r == null) {
            return new ResourceRequirementsBuilder().build();
        }
        return new ResourceRequirementsBuilder()
                .withRequests(quantities(r.getRequests()))
                .withLimits(quantities(r.getLimits()))
                .build();
    }

    private static Map<String, Quantity> quantities(Map<String, String> in) {
        if (in == null) {
            return Map.of();
        }
        Map<String, Quantity> out = new LinkedHashMap<>();
        in.forEach((k, v) -> out.put(k, Quantity.parse(v)));
        return out;
    }
}
