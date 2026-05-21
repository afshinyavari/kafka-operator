package se.afshin.yavari.kafka.operator.externalaccess;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpRouteBuilderTest {

    private static final String NAME = "kafka-ui";
    private static final String NS = "kafka";
    private static final Map<String, String> LABELS = Map.of("app", NAME);

    private final HttpRouteBuilder builder = new HttpRouteBuilder();

    @Test
    void buildsSingleHostnameRoute() {
        HttpGatewayConfig gw = new HttpGatewayConfig();
        gw.setParentGatewayName("external-gw");
        gw.setParentGatewayNamespace("gateway-system");
        gw.setSectionName("https");

        GenericKubernetesResource route = builder.build(NAME, NS, LABELS, null,
                "kafka-ui.example.com", NAME, 8080, gw);

        assertThat(route.getApiVersion()).isEqualTo(HttpRouteBuilder.API_VERSION);
        assertThat(route.getKind()).isEqualTo(HttpRouteBuilder.KIND);
        assertThat(route.getMetadata().getName()).isEqualTo(NAME);
        assertThat(route.getMetadata().getNamespace()).isEqualTo(NS);

        Map<String, Object> spec = (Map<String, Object>) route.getAdditionalProperties().get("spec");
        assertThat(spec).isNotNull();
        assertThat((List<String>) spec.get("hostnames")).containsExactly("kafka-ui.example.com");

        List<Map<String, Object>> parents = (List<Map<String, Object>>) spec.get("parentRefs");
        assertThat(parents).hasSize(1);
        assertThat(parents.get(0)).containsEntry("name", "external-gw");
        assertThat(parents.get(0)).containsEntry("namespace", "gateway-system");
        assertThat(parents.get(0)).containsEntry("sectionName", "https");

        List<Map<String, Object>> rules = (List<Map<String, Object>>) spec.get("rules");
        assertThat(rules).hasSize(1);
        List<Map<String, Object>> backends = (List<Map<String, Object>>) rules.get(0).get("backendRefs");
        assertThat(backends.get(0)).containsEntry("name", NAME);
        assertThat(backends.get(0)).containsEntry("port", 8080);
    }

    @Test
    void defaultsParentNamespaceToCrNamespace() {
        HttpGatewayConfig gw = new HttpGatewayConfig();
        gw.setParentGatewayName("external-gw");

        GenericKubernetesResource route = builder.build(NAME, NS, LABELS, null,
                "kafka-ui.example.com", NAME, 8080, gw);
        Map<String, Object> spec = (Map<String, Object>) route.getAdditionalProperties().get("spec");
        List<Map<String, Object>> parents = (List<Map<String, Object>>) spec.get("parentRefs");
        assertThat(parents.get(0)).containsEntry("namespace", NS);
    }

    @Test
    void missingParentGateway_throws() {
        assertThatThrownBy(() -> builder.build(NAME, NS, LABELS, null,
                "kafka-ui.example.com", NAME, 8080, new HttpGatewayConfig()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parentGatewayName");
    }

    @Test
    void blankHost_throws() {
        HttpGatewayConfig gw = new HttpGatewayConfig();
        gw.setParentGatewayName("external-gw");
        assertThatThrownBy(() -> builder.build(NAME, NS, LABELS, null, "", NAME, 8080, gw))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertised host");
    }
}
