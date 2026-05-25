package se.afshin.yavari.kafka.operator.crd;

import java.util.ArrayList;
import java.util.List;

/** Three composable channels by which Connect workers receive plugin JARs. All three
 *  can be combined; the operator composes a single {@code plugin.path} that points at
 *  the operator-shipped {@code /opt/kafka/connect-plugins/baked} dir plus a per-channel
 *  mount point for each configured source. */
public class KafkaConnectPluginSources {

    /** Name of a PVC in the same namespace. Mounted read-only at
     *  {@code /opt/kafka/connect-plugins/pvc/}. The user owns populating the PVC
     *  (Job, kubectl cp, CSI driver). */
    private String pluginsVolumeClaim;

    /** ConfigMap names in the same namespace. Each mounts read-only at
     *  {@code /opt/kafka/connect-plugins/cm-<name>/}. ~1 MiB cap per ConfigMap; suitable
     *  for small SMT JARs, too small for most connectors. */
    private List<String> pluginConfigMaps = new ArrayList<>();

    /** Secret names in the same namespace. Each mounts read-only at
     *  {@code /opt/kafka/connect-plugins/secret-<name>/}. Same size limits as ConfigMap. */
    private List<String> pluginSecrets = new ArrayList<>();

    public String getPluginsVolumeClaim() { return pluginsVolumeClaim; }
    public void setPluginsVolumeClaim(String pluginsVolumeClaim) { this.pluginsVolumeClaim = pluginsVolumeClaim; }

    public List<String> getPluginConfigMaps() { return pluginConfigMaps; }
    public void setPluginConfigMaps(List<String> pluginConfigMaps) { this.pluginConfigMaps = pluginConfigMaps; }

    public List<String> getPluginSecrets() { return pluginSecrets; }
    public void setPluginSecrets(List<String> pluginSecrets) { this.pluginSecrets = pluginSecrets; }
}
