package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.VolumeMount;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.infra.SecurityContextDefaults;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the Apicurio schema export/import container. Backups run the export as a second
 * (one-shot) container in the CronJob pod; restores run the import as an init container so
 * schemas are present before records land. Both use the {@code kafka-backup} image, which
 * bundles {@code apicurio-export.sh} / {@code apicurio-import.sh}.
 */
@ApplicationScoped
public class SchemaBackupStep {

    public static final String EXPORT_CONTAINER = "schema-export";
    public static final String IMPORT_CONTAINER = "schema-import";

    /** Schema-export sidecar for a backup pod. */
    public Container exportSidecar(String image, String pullPolicy, String apicurioUrl,
                                   String schemaLocation, String authSecretRef,
                                   List<EnvVar> mcEnv, List<VolumeMount> storageMounts,
                                   ResourceRequirements resources) {
        return build(EXPORT_CONTAINER, "apicurio-export.sh", "SCHEMA_DEST",
                image, pullPolicy, apicurioUrl, schemaLocation, authSecretRef,
                mcEnv, storageMounts, resources);
    }

    /** Schema-import init container for a restore pod. */
    public Container importInitContainer(String image, String pullPolicy, String apicurioUrl,
                                         String schemaLocation, String authSecretRef,
                                         List<EnvVar> mcEnv, List<VolumeMount> storageMounts,
                                         ResourceRequirements resources) {
        return build(IMPORT_CONTAINER, "apicurio-import.sh", "SCHEMA_SRC",
                image, pullPolicy, apicurioUrl, schemaLocation, authSecretRef,
                mcEnv, storageMounts, resources);
    }

    private Container build(String name, String script, String locationEnv,
                            String image, String pullPolicy, String apicurioUrl,
                            String schemaLocation, String authSecretRef,
                            List<EnvVar> mcEnv, List<VolumeMount> storageMounts,
                            ResourceRequirements resources) {
        List<EnvVar> env = new ArrayList<>();
        env.add(plain("APICURIO_URL", apicurioUrl));
        env.add(plain(locationEnv, schemaLocation));
        if (mcEnv != null) {
            env.addAll(mcEnv);
        }
        if (authSecretRef != null && !authSecretRef.isBlank()) {
            env.add(optionalSecret("OAUTH_TOKEN_URL", authSecretRef, "token-url"));
            env.add(optionalSecret("OAUTH_CLIENT_ID", authSecretRef, "client-id"));
            env.add(optionalSecret("OAUTH_CLIENT_SECRET", authSecretRef, "client-secret"));
        }
        return new ContainerBuilder()
                .withName(name)
                .withImage(image)
                .withImagePullPolicy(pullPolicy)
                .withCommand(script)
                .withEnv(env)
                .withVolumeMounts(storageMounts)
                .withResources(resources)
                .withSecurityContext(SecurityContextDefaults.containerDefaults())
                .build();
    }

    private static EnvVar plain(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
    }

    private static EnvVar optionalSecret(String name, String secretName, String key) {
        return new EnvVarBuilder()
                .withName(name)
                .withNewValueFrom()
                    .withNewSecretKeyRef(key, secretName, true)
                .endValueFrom()
                .build();
    }
}
