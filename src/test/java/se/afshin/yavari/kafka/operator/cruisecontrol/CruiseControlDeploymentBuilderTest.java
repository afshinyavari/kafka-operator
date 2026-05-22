package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlDeploymentBuilderTest {

    private final CruiseControlDeploymentBuilder builder = new CruiseControlDeploymentBuilder();

    private Container container(boolean metricsEnabled) {
        KafkaClusterCruiseControlSpec spec = new KafkaClusterCruiseControlSpec();
        Deployment dep = builder.build(spec, "kafka", "hash", false, null,
                "kafka:img", metricsEnabled);
        return dep.getSpec().getTemplate().getSpec().getContainers().get(0);
    }

    @Test
    void metricsEnabledAttachesJavaagentAndExposesPort() {
        Container c = container(true);
        assertThat(c.getEnv()).extracting(EnvVar::getName)
                .contains("KAFKA_OPTS", "KAFKA_HEAP_OPTS");
        assertThat(c.getEnv()).anySatisfy(e -> {
            if ("KAFKA_OPTS".equals(e.getName())) {
                assertThat(e.getValue())
                        .contains("-javaagent:/opt/jmx-exporter/jmx-exporter.jar=9101:")
                        .contains("/etc/cruise-control/jmx-config.yaml");
            }
        });
        assertThat(c.getPorts()).extracting(ContainerPort::getName)
                .contains("cc-rest", "metrics");
        assertThat(c.getPorts()).anySatisfy(p -> {
            if ("metrics".equals(p.getName())) {
                assertThat(p.getContainerPort()).isEqualTo(9101);
            }
        });
    }

    @Test
    void metricsDisabledHasNoJavaagentAndNoMetricsPort() {
        Container c = container(false);
        assertThat(c.getEnv()).extracting(EnvVar::getName)
                .contains("KAFKA_HEAP_OPTS")
                .doesNotContain("KAFKA_OPTS");
        assertThat(c.getPorts()).extracting(ContainerPort::getName)
                .contains("cc-rest")
                .doesNotContain("metrics");
    }
}
