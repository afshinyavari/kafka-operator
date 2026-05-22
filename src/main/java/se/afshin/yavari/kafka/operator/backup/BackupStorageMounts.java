package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.AzureStorageConfig;
import se.afshin.yavari.kafka.operator.crd.BackupStorageSpec;
import se.afshin.yavari.kafka.operator.crd.GcsStorageConfig;
import se.afshin.yavari.kafka.operator.crd.PvcStorageConfig;
import se.afshin.yavari.kafka.operator.crd.S3StorageConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a {@link BackupStorageSpec} into pod wiring for the kafka-backup container
 * (volumes / mounts / env) and renders the {@code storage:} block of the config file.
 *
 * <p>Storage credentials are supplied as Secret-backed env vars or a mounted Secret file —
 * never inlined into the rendered config.
 */
@ApplicationScoped
public class BackupStorageMounts {

    private static final String GCS_VOLUME = "gcs-key";

    /** Pod wiring for the kafka-backup container. */
    public record StorageWiring(List<Volume> volumes, List<VolumeMount> mounts, List<EnvVar> env) {}

    public StorageWiring forBackupTool(BackupStorageSpec storage) {
        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        List<EnvVar> env = new ArrayList<>();

        switch (storage.resolveType()) {
            case PVC -> {
                PvcStorageConfig pvc = storage.getPvc();
                volumes.add(new VolumeBuilder()
                        .withName("backup-storage")
                        .withNewPersistentVolumeClaim()
                            .withClaimName(pvc.getClaimName())
                        .endPersistentVolumeClaim()
                        .build());
                VolumeMountBuilder mount = new VolumeMountBuilder()
                        .withName("backup-storage")
                        .withMountPath(BackupPaths.PVC_DIR);
                if (pvc.getSubPath() != null && !pvc.getSubPath().isBlank()) {
                    mount.withSubPath(pvc.getSubPath());
                }
                mounts.add(mount.build());
            }
            case S3 -> {
                S3StorageConfig s3 = storage.getS3();
                env.add(secretEnv("AWS_ACCESS_KEY_ID", s3.getCredentialsSecretRef(), "accessKeyId"));
                env.add(secretEnv("AWS_SECRET_ACCESS_KEY", s3.getCredentialsSecretRef(), "secretAccessKey"));
                env.add(plainEnv("AWS_REGION", s3.getRegion()));
                if (s3.getEndpoint() != null && !s3.getEndpoint().isBlank()) {
                    env.add(plainEnv("AWS_ENDPOINT", s3.getEndpoint()));
                    if (s3.getEndpoint().startsWith("http://")) {
                        env.add(plainEnv("AWS_ALLOW_HTTP", "true"));
                    }
                }
            }
            case AZURE -> {
                AzureStorageConfig az = storage.getAzure();
                env.add(secretEnv("AZURE_STORAGE_ACCOUNT_NAME", az.getCredentialsSecretRef(), "accountName"));
                env.add(secretEnv("AZURE_STORAGE_ACCOUNT_KEY", az.getCredentialsSecretRef(), "accountKey"));
            }
            case GCS -> {
                GcsStorageConfig gcs = storage.getGcs();
                volumes.add(new VolumeBuilder()
                        .withName(GCS_VOLUME)
                        .withNewSecret().withSecretName(gcs.getCredentialsSecretRef()).endSecret()
                        .build());
                mounts.add(new VolumeMountBuilder()
                        .withName(GCS_VOLUME)
                        .withMountPath(BackupPaths.GCS_DIR)
                        .withReadOnly(true)
                        .build());
                env.add(plainEnv("GOOGLE_APPLICATION_CREDENTIALS", BackupPaths.GCS_KEY_FILE));
            }
        }
        return new StorageWiring(volumes, mounts, env);
    }

    /** Renders the {@code storage:} block of a kafka-backup config (no credentials). */
    public static void appendStorageBlock(StringBuilder sb, BackupStorageSpec storage) {
        sb.append("storage:\n");
        switch (storage.resolveType()) {
            case PVC -> {
                sb.append("  backend: filesystem\n");
                sb.append("  path: ").append(BackupYaml.q(BackupPaths.PVC_DIR)).append('\n');
            }
            case S3 -> {
                S3StorageConfig s3 = storage.getS3();
                sb.append("  backend: s3\n");
                sb.append("  bucket: ").append(BackupYaml.q(s3.getBucket())).append('\n');
                sb.append("  region: ").append(BackupYaml.q(s3.getRegion())).append('\n');
                sb.append("  prefix: ").append(BackupYaml.q(nullToEmpty(s3.getPrefix()))).append('\n');
                if (s3.getEndpoint() != null && !s3.getEndpoint().isBlank()) {
                    sb.append("  endpoint: ").append(BackupYaml.q(s3.getEndpoint())).append('\n');
                }
                if (s3.isPathStyleAccess()) {
                    sb.append("  path_style_access: true\n");
                }
            }
            case AZURE -> {
                AzureStorageConfig az = storage.getAzure();
                sb.append("  backend: azure\n");
                sb.append("  container: ").append(BackupYaml.q(az.getContainer())).append('\n');
                sb.append("  prefix: ").append(BackupYaml.q(nullToEmpty(az.getPrefix()))).append('\n');
            }
            case GCS -> {
                GcsStorageConfig gcs = storage.getGcs();
                sb.append("  backend: gcs\n");
                sb.append("  bucket: ").append(BackupYaml.q(gcs.getBucket())).append('\n');
                sb.append("  prefix: ").append(BackupYaml.q(nullToEmpty(gcs.getPrefix()))).append('\n');
            }
        }
    }

    /** Whether the Apicurio schema export/import is supported for this backend in v1
     *  (PVC and S3-compatible only — the helper scripts use a filesystem path or `mc`). */
    public static boolean schemaExportSupported(BackupStorageSpec storage) {
        return switch (storage.resolveType()) {
            case PVC, S3 -> true;
            case AZURE, GCS -> false;
        };
    }

    /** SCHEMA_DEST / SCHEMA_SRC value for the schema helper scripts. PVC → an absolute
     *  path; S3 → a {@code <bucket>/<prefix>/schemas} object-storage location. */
    public static String schemaLocation(BackupStorageSpec storage) {
        return switch (storage.resolveType()) {
            case PVC -> BackupPaths.PVC_DIR + "/" + BackupPaths.SCHEMA_SUBDIR;
            case S3 -> {
                S3StorageConfig s3 = storage.getS3();
                String prefix = nullToEmpty(s3.getPrefix());
                StringBuilder loc = new StringBuilder(s3.getBucket());
                if (!prefix.isBlank()) {
                    loc.append('/').append(trimSlashes(prefix));
                }
                loc.append('/').append(BackupPaths.SCHEMA_SUBDIR);
                yield loc.toString();
            }
            case AZURE, GCS -> throw new IllegalStateException(
                    "schema export is not supported for azure/gcs storage in v1");
        };
    }

    /** Env vars the schema helper scripts need to reach object storage via `mc`
     *  (empty for PVC storage, which uses a filesystem path). */
    public List<EnvVar> schemaToolEnv(BackupStorageSpec storage) {
        List<EnvVar> env = new ArrayList<>();
        if (storage.resolveType() == se.afshin.yavari.kafka.operator.crd.BackupStorageType.S3) {
            S3StorageConfig s3 = storage.getS3();
            String url = (s3.getEndpoint() != null && !s3.getEndpoint().isBlank())
                    ? s3.getEndpoint()
                    : "https://s3." + s3.getRegion() + ".amazonaws.com";
            env.add(plainEnv("MC_ALIAS_URL", url));
            env.add(secretEnv("MC_ALIAS_ACCESS_KEY", s3.getCredentialsSecretRef(), "accessKeyId"));
            env.add(secretEnv("MC_ALIAS_SECRET_KEY", s3.getCredentialsSecretRef(), "secretAccessKey"));
        }
        return env;
    }

    static EnvVar plainEnv(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    static EnvVar secretEnv(String name, String secretName, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewSecretKeyRef(key, secretName, false)
                .endValueFrom()
                .build();
    }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    private static String trimSlashes(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && s.charAt(start) == '/') start++;
        while (end > start && s.charAt(end - 1) == '/') end--;
        return s.substring(start, end);
    }
}
