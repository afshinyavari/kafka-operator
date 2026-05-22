package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlConfigMapBuilderTest {

    private final CruiseControlConfigMapBuilder builder = new CruiseControlConfigMapBuilder();

    @Test
    void includesJmxConfigKeyWhenProvided() {
        ConfigMap cm = builder.build("kafka", "props", "{}", "jmx-yaml-content");
        assertThat(cm.getData()).containsEntry("jmx-config.yaml", "jmx-yaml-content");
        assertThat(cm.getData()).containsKey("cruisecontrol.properties");
        assertThat(cm.getData()).containsKey("capacity.json");
        assertThat(cm.getData()).containsKey("clusterConfigs.json");
    }

    @Test
    void omitsJmxConfigKeyWhenNull() {
        ConfigMap cm = builder.build("kafka", "props", "{}", null);
        assertThat(cm.getData()).doesNotContainKey("jmx-config.yaml");
        assertThat(cm.getData()).containsKey("cruisecontrol.properties");
    }
}
