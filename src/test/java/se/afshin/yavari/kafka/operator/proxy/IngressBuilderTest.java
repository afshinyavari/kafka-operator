package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyIngressConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngressBuilderTest {

    private static final String NS = "kafka";
    private static final String NAME = "kafka-proxy";
    private static final String HOST = "a.kafka.example.com";

    private IngressBuilder builder;

    @BeforeEach
    void setup() {
        builder = new IngressBuilder();
    }

    @Test
    void build_metadataAndAnnotations() {
        Ingress ing = builder.build(proxy(null), 1, NS, resolved());

        assertThat(ing.getMetadata().getName()).isEqualTo(NAME);
        assertThat(ing.getMetadata().getNamespace()).isEqualTo(NS);
        assertThat(ing.getMetadata().getAnnotations())
                .containsEntry("nginx.ingress.kubernetes.io/ssl-passthrough", "true")
                .containsEntry("nginx.ingress.kubernetes.io/backend-protocol", "HTTPS");
    }

    @Test
    void build_perBrokerRules() {
        Ingress ing = builder.build(proxy(null), 3, NS, resolved());

        List<IngressRule> rules = ing.getSpec().getRules();
        assertThat(rules).hasSize(4); // bootstrap + 3 brokers
        assertThat(rules.get(0).getHost()).isEqualTo("bootstrap." + HOST);
        assertThat(rules.get(1).getHost()).isEqualTo("broker-0." + HOST);
        assertThat(rules.get(2).getHost()).isEqualTo("broker-1." + HOST);
        assertThat(rules.get(3).getHost()).isEqualTo("broker-2." + HOST);
    }

    @Test
    void build_rulesUseBrokerNodeIdRanges() {
        KafkaProxy p = proxy(null);
        p.getSpec().setBrokerNodeIdRanges(List.of(
                range("brokers-a", 0, 1),
                range("brokers-b", 1000, 1000)));

        Ingress ing = builder.build(p, 0, NS, resolved());

        List<String> hosts = ing.getSpec().getRules().stream().map(IngressRule::getHost).toList();
        assertThat(hosts).containsExactly(
                "bootstrap." + HOST,
                "broker-0." + HOST,
                "broker-1." + HOST,
                "broker-1000." + HOST);
    }

    @Test
    void build_backendPointsAtProxyService() {
        KafkaProxy p = proxy(null);
        p.getSpec().setClientPort(9094);

        Ingress ing = builder.build(p, 1, NS, resolved());

        var rule = ing.getSpec().getRules().get(0);
        var backend = rule.getHttp().getPaths().get(0).getBackend();
        assertThat(backend.getService().getName()).isEqualTo(NAME);
        assertThat(backend.getService().getPort().getNumber()).isEqualTo(9094);
    }

    @Test
    void build_ingressClassNameWhenProvided() {
        Ingress ing = builder.build(proxy(ingressConfig("nginx")), 1, NS, resolved());

        assertThat(ing.getSpec().getIngressClassName()).isEqualTo("nginx");
    }

    @Test
    void build_omitsIngressClassNameWhenAbsent() {
        Ingress ing = builder.build(proxy(null), 1, NS, resolved());

        // Should be null (controller chooses default ingress class).
        assertThat(ing.getSpec().getIngressClassName()).isNull();
    }

    @Test
    void build_unresolvedHostThrows() {
        assertThatThrownBy(() -> builder.build(proxy(null), 1, NS,
                ExternalAccessResolution.pending(ExternalAccessType.INGRESS)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertisedHost");
    }

    // --- helpers ---

    private KafkaProxy proxy(KafkaProxyIngressConfig ing) {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setClientPort(9094);
        KafkaProxyExternalAccessConfig ea = new KafkaProxyExternalAccessConfig();
        ea.setType(ExternalAccessType.INGRESS);
        ea.setIngress(ing);
        spec.setExternalAccess(ea);
        p.setSpec(spec);
        return p;
    }

    private KafkaProxyIngressConfig ingressConfig(String className) {
        KafkaProxyIngressConfig ing = new KafkaProxyIngressConfig();
        ing.setIngressClassName(className);
        return ing;
    }

    private BrokerNodeIdRange range(String name, int start, int end) {
        BrokerNodeIdRange r = new BrokerNodeIdRange();
        r.setName(name);
        r.setStart(start);
        r.setEnd(end);
        return r;
    }

    private static ExternalAccessResolution resolved() {
        return ExternalAccessResolution.resolved(ExternalAccessType.INGRESS, HOST);
    }
}
