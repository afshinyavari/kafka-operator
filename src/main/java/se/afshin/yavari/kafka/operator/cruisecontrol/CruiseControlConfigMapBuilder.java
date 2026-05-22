package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Builds the {@code cruise-control-config} ConfigMap holding the rendered
 * {@code cruisecontrol.properties} and {@code capacity.json}. Mounted into the Cruise
 * Control container at {@link CruiseControlConfigBuilder#CONFIG_DIR}.
 */
@ApplicationScoped
public class CruiseControlConfigMapBuilder {

    /**
     * @param jmxConfigYaml the bundled JMX exporter config, or {@code null} when metrics are
     *                      disabled — when non-null it is added as a {@code jmx-config.yaml}
     *                      key, surfacing at {@code CONFIG_DIR/jmx-config.yaml} in the pod.
     */
    public ConfigMap build(String namespace, String ccProperties, String capacityJson,
                           String jmxConfigYaml) {
        ConfigMapBuilder builder = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(CruiseControlOrchestrator.CONFIG_MAP_NAME)
                    .withNamespace(namespace)
                    .withLabels(CruiseControlDeploymentBuilder.labels())
                .endMetadata()
                .addToData("cruisecontrol.properties", ccProperties)
                .addToData("capacity.json", capacityJson)
                // clusterConfigs.json must exist for some Cruise Control endpoints; an
                // empty object is a valid no-override file.
                .addToData("clusterConfigs.json", "{}");
        if (jmxConfigYaml != null) {
            builder.addToData("jmx-config.yaml", jmxConfigYaml);
        }
        return builder.build();
    }
}
