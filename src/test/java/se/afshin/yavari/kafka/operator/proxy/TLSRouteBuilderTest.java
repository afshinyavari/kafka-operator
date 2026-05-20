package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyGatewayConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TLSRouteBuilderTest {

    private static final String NS = "kafka";
    private static final String NAME = "kafka-proxy";
    private static final String HOST = "a.kafka.example.com";

    private TLSRouteBuilder builder;

    @BeforeEach
    void setup() {
        builder = new TLSRouteBuilder();
    }

    @Test
    void build_metadata() {
        GenericKubernetesResource route = builder.build(
                proxy(gateway("kafka-gw", "gateway-system", null)), 2, NS, resolved());

        assertThat(route.getApiVersion()).isEqualTo("gateway.networking.k8s.io/v1alpha2");
        assertThat(route.getKind()).isEqualTo("TLSRoute");
        assertThat(route.getMetadata().getName()).isEqualTo(NAME);
        assertThat(route.getMetadata().getNamespace()).isEqualTo(NS);
    }

    @Test
    void build_parentRef_wiredCorrectly() {
        GenericKubernetesResource route = builder.build(
                proxy(gateway("kafka-gw", "gateway-system", "tls-listener")), 1, NS, resolved());

        Map<String, Object> spec = spec(route);
        List<Map<String, Object>> parents = list(spec, "parentRefs");
        assertThat(parents).hasSize(1);
        assertThat(parents.get(0))
                .containsEntry("name", "kafka-gw")
                .containsEntry("namespace", "gateway-system")
                .containsEntry("sectionName", "tls-listener");
    }

    @Test
    void build_parentNamespaceDefaultsToProxyNamespace() {
        GenericKubernetesResource route = builder.build(
                proxy(gateway("kafka-gw", null, null)), 1, NS, resolved());

        Map<String, Object> parent = list(spec(route), "parentRefs").get(0);
        assertThat(parent).containsEntry("namespace", NS);
        // No section name when not set.
        assertThat(parent).doesNotContainKey("sectionName");
    }

    @Test
    void build_hostnamesIncludeBootstrapAndPerBroker() {
        GenericKubernetesResource route = builder.build(
                proxy(gateway("kafka-gw", null, null)), 3, NS, resolved());

        @SuppressWarnings("unchecked")
        List<String> hostnames = (List<String>) spec(route).get("hostnames");
        assertThat(hostnames).containsExactly(
                "bootstrap." + HOST,
                "broker-0." + HOST,
                "broker-1." + HOST,
                "broker-2." + HOST);
    }

    @Test
    void build_hostnamesFromBrokerNodeIdRanges() {
        KafkaProxy p = proxy(gateway("kafka-gw", null, null));
        p.getSpec().setBrokerNodeIdRanges(List.of(
                range("brokers-a", 0, 1),
                range("brokers-b", 1000, 1000)));

        GenericKubernetesResource route = builder.build(p, 0, NS, resolved());

        @SuppressWarnings("unchecked")
        List<String> hostnames = (List<String>) spec(route).get("hostnames");
        assertThat(hostnames).containsExactly(
                "bootstrap." + HOST,
                "broker-0." + HOST,
                "broker-1." + HOST,
                "broker-1000." + HOST);
    }

    @Test
    void build_backendRefPointsAtProxyService() {
        KafkaProxy p = proxy(gateway("kafka-gw", null, null));
        p.getSpec().setClientPort(9094);

        GenericKubernetesResource route = builder.build(p, 1, NS, resolved());

        Map<String, Object> rule = list(spec(route), "rules").get(0);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> backends = (List<Map<String, Object>>) rule.get("backendRefs");
        assertThat(backends).hasSize(1);
        assertThat(backends.get(0))
                .containsEntry("name", NAME)
                .containsEntry("port", 9094);
    }

    @Test
    void build_missingParentGatewayNameThrows() {
        KafkaProxy p = proxy(gateway(null, null, null));

        assertThatThrownBy(() -> builder.build(p, 1, NS, resolved()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parentGatewayName is required");
    }

    @Test
    void build_unresolvedHostThrows() {
        KafkaProxy p = proxy(gateway("kafka-gw", null, null));

        assertThatThrownBy(() -> builder.build(p, 1, NS,
                ExternalAccessResolution.pending(ExternalAccessType.GATEWAY)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertisedHost");
    }

    // --- helpers ---

    private KafkaProxy proxy(KafkaProxyGatewayConfig gw) {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setClientPort(9094);
        KafkaProxyExternalAccessConfig ea = new KafkaProxyExternalAccessConfig();
        ea.setType(ExternalAccessType.GATEWAY);
        ea.setGateway(gw);
        spec.setExternalAccess(ea);
        p.setSpec(spec);
        return p;
    }

    private KafkaProxyGatewayConfig gateway(String name, String namespace, String sectionName) {
        KafkaProxyGatewayConfig gw = new KafkaProxyGatewayConfig();
        gw.setParentGatewayName(name);
        gw.setParentGatewayNamespace(namespace);
        gw.setSectionName(sectionName);
        return gw;
    }

    private BrokerNodeIdRange range(String name, int start, int end) {
        BrokerNodeIdRange r = new BrokerNodeIdRange();
        r.setName(name);
        r.setStart(start);
        r.setEnd(end);
        return r;
    }

    private static ExternalAccessResolution resolved() {
        return ExternalAccessResolution.resolved(ExternalAccessType.GATEWAY, HOST);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> spec(GenericKubernetesResource r) {
        return (Map<String, Object>) r.getAdditionalProperties().get("spec");
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> list(Map<String, Object> m, String key) {
        return (List<Map<String, Object>>) m.get(key);
    }
}
