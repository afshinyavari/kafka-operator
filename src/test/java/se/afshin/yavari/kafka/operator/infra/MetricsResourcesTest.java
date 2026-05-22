package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsResourcesTest {

    private final MetricsResources metricsResources = new MetricsResources();

    private List<OwnerReference> owner() {
        return List.of(new OwnerReferenceBuilder().withName("my-cluster").withUid("u")
                .withKind("KafkaCluster").withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withController(true).build());
    }

    @Test
    void metricsServiceShape() {
        Service svc = metricsResources.metricsService(
                "kafka-proxy", "kafka",
                Map.of("app", "kroxylicious"),
                Map.of("app", "kroxylicious", "pod", "x"),
                "metrics", 9190, owner());

        assertThat(svc.getMetadata().getName()).isEqualTo("kafka-proxy-metrics");
        assertThat(svc.getMetadata().getNamespace()).isEqualTo("kafka");
        assertThat(svc.getMetadata().getLabels()).containsEntry("app", "kroxylicious");
        assertThat(svc.getMetadata().getOwnerReferences()).hasSize(1);
        assertThat(svc.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(svc.getSpec().getSelector()).containsEntry("pod", "x");
        assertThat(svc.getSpec().getPorts()).hasSize(1);
        ServicePort port = svc.getSpec().getPorts().get(0);
        assertThat(port.getName()).isEqualTo("metrics");
        assertThat(port.getPort()).isEqualTo(9190);
        assertThat(port.getTargetPort().getIntVal()).isEqualTo(9190);
    }

    @Test
    @SuppressWarnings("unchecked")
    void serviceMonitorShape() {
        GenericKubernetesResource sm = metricsResources.serviceMonitor(
                "cruise-control", "kafka",
                Map.of("app", "cruise-control"),
                Map.of("app", "cruise-control"),
                "metrics", owner());

        assertThat(sm.getApiVersion()).isEqualTo("monitoring.coreos.com/v1");
        assertThat(sm.getKind()).isEqualTo("ServiceMonitor");
        assertThat(sm.getMetadata().getName()).isEqualTo("cruise-control-metrics");
        assertThat(sm.getMetadata().getOwnerReferences()).hasSize(1);

        Map<String, Object> spec = (Map<String, Object>) sm.getAdditionalProperties().get("spec");
        assertThat(spec).isNotNull();
        Map<String, Object> selector = (Map<String, Object>) spec.get("selector");
        Map<String, String> matchLabels = (Map<String, String>) selector.get("matchLabels");
        assertThat(matchLabels).containsEntry("app", "cruise-control");
        List<Map<String, Object>> endpoints = (List<Map<String, Object>>) spec.get("endpoints");
        assertThat(endpoints).hasSize(1);
        assertThat(endpoints.get(0)).containsEntry("port", "metrics");
        assertThat(endpoints.get(0)).containsEntry("interval", "30s");
    }

    @Test
    @SuppressWarnings("unchecked")
    void serviceMonitorHonoursCustomInterval() {
        GenericKubernetesResource sm = metricsResources.serviceMonitor(
                "x", "kafka", Map.of("a", "b"), Map.of("a", "b"), "metrics", "60s", owner());
        Map<String, Object> spec = (Map<String, Object>) sm.getAdditionalProperties().get("spec");
        List<Map<String, Object>> endpoints = (List<Map<String, Object>>) spec.get("endpoints");
        assertThat(endpoints.get(0)).containsEntry("interval", "60s");
    }

    @Test
    void jmxConfigLoadsBundledResources() {
        assertThat(MetricsResources.jmxConfig("cruise-control-jmx-config.yaml"))
                .contains("rules:");
        assertThat(MetricsResources.jmxConfig("connect-jmx-config.yaml"))
                .contains("rules:");
    }

    @Test
    void jmxConfigMissingResourceThrows() {
        assertThatThrownBy(() -> MetricsResources.jmxConfig("no-such-file.yaml"))
                .isInstanceOf(IllegalStateException.class);
    }
}
