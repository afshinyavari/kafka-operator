package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectPluginSources;

import java.util.ArrayList;
import java.util.List;

/**
 * Resolves the three plugin-delivery channels (PVC + ConfigMaps + Secrets) against the
 * Kubernetes API. Confirms each referenced object exists (missing → IllegalStateException
 * with a precise message that becomes the FAILED phase message). Returns the composed
 * {@code plugin.path} and the Volume/VolumeMount pairs the Deployment must wire in.
 *
 * <p>The operator's image always carries a {@code /opt/kafka/connect-plugins/baked} dir
 * — that's the fourth (implicit) channel where users baking plugins into a custom image
 * land. This dir is always appended to {@code plugin.path} regardless of spec configuration.
 */
@ApplicationScoped
public class ConnectPluginResolver {

    public static final String PLUGIN_ROOT = "/opt/kafka/connect-plugins";
    public static final String BAKED_DIR = PLUGIN_ROOT + "/baked";
    public static final String PVC_DIR = PLUGIN_ROOT + "/pvc";
    public static final String CM_PREFIX = PLUGIN_ROOT + "/cm-";
    public static final String SECRET_PREFIX = PLUGIN_ROOT + "/secret-";

    @Inject KubernetesClient client;

    public ResolvedPluginSources resolve(KafkaConnectPluginSources sources, String namespace) {
        List<Volume> volumes = new ArrayList<>();
        List<VolumeMount> mounts = new ArrayList<>();
        List<String> secretNames = new ArrayList<>();
        List<String> pathDirs = new ArrayList<>();
        pathDirs.add(BAKED_DIR);

        if (sources == null) {
            return new ResolvedPluginSources(String.join(",", pathDirs),
                    volumes, mounts, secretNames);
        }

        String pvc = sources.getPluginsVolumeClaim();
        if (pvc != null && !pvc.isBlank()) {
            requirePvc(pvc, namespace);
            volumes.add(new VolumeBuilder()
                    .withName("plugins-pvc")
                    .withNewPersistentVolumeClaim()
                        .withClaimName(pvc).withReadOnly(true)
                    .endPersistentVolumeClaim()
                    .build());
            mounts.add(new VolumeMountBuilder()
                    .withName("plugins-pvc")
                    .withMountPath(PVC_DIR)
                    .withReadOnly(true).build());
            pathDirs.add(PVC_DIR);
        }

        if (sources.getPluginConfigMaps() != null) {
            int i = 0;
            for (String cmName : sources.getPluginConfigMaps()) {
                requireConfigMap(cmName, namespace, i);
                String volName = "plugins-cm-" + i;
                String mountPath = CM_PREFIX + cmName;
                volumes.add(new VolumeBuilder()
                        .withName(volName)
                        .withNewConfigMap().withName(cmName).endConfigMap()
                        .build());
                mounts.add(new VolumeMountBuilder()
                        .withName(volName)
                        .withMountPath(mountPath)
                        .withReadOnly(true).build());
                pathDirs.add(mountPath);
                i++;
            }
        }

        if (sources.getPluginSecrets() != null) {
            int i = 0;
            for (String secName : sources.getPluginSecrets()) {
                requireSecret(secName, namespace, i);
                String volName = "plugins-secret-" + i;
                String mountPath = SECRET_PREFIX + secName;
                volumes.add(new VolumeBuilder()
                        .withName(volName)
                        .withNewSecret().withSecretName(secName).endSecret()
                        .build());
                mounts.add(new VolumeMountBuilder()
                        .withName(volName)
                        .withMountPath(mountPath)
                        .withReadOnly(true).build());
                pathDirs.add(mountPath);
                secretNames.add(secName);
                i++;
            }
        }

        return new ResolvedPluginSources(String.join(",", pathDirs),
                volumes, mounts, secretNames);
    }

    private void requirePvc(String name, String namespace) {
        if (client.persistentVolumeClaims().inNamespace(namespace).withName(name).get() == null) {
            throw new IllegalStateException("spec.pluginSources.pluginsVolumeClaim='" + name
                    + "' not found in namespace " + namespace);
        }
    }

    private void requireConfigMap(String name, String namespace, int index) {
        if (client.configMaps().inNamespace(namespace).withName(name).get() == null) {
            throw new IllegalStateException("spec.pluginSources.pluginConfigMaps[" + index
                    + "]='" + name + "' not found in namespace " + namespace);
        }
    }

    private void requireSecret(String name, String namespace, int index) {
        if (client.secrets().inNamespace(namespace).withName(name).get() == null) {
            throw new IllegalStateException("spec.pluginSources.pluginSecrets[" + index
                    + "]='" + name + "' not found in namespace " + namespace);
        }
    }
}
