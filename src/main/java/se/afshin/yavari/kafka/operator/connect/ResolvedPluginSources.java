package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.util.List;

/** Result of resolving a {@code KafkaConnectPluginSources} block against the K8s API:
 *  the composed {@code plugin.path}, the per-source Volumes and VolumeMounts that the
 *  Deployment must wire in, and the Secret names that feed the {@code SecretRevisionTracker}
 *  (so plugin-Secret rotations roll the pods). */
public record ResolvedPluginSources(
        String pluginPath,
        List<Volume> volumes,
        List<VolumeMount> volumeMounts,
        List<String> referencedSecretNames
) {}
