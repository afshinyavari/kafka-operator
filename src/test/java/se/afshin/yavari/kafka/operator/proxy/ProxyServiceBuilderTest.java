package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyServiceBuilderTest {

    private static final String NS = "kafka";
    private static final String PROXY_NAME = "kafka-proxy";
    private static final int CLIENT_PORT = 9094;

    private ProxyServiceBuilder builder;

    @BeforeEach
    void setup() {
        builder = new ProxyServiceBuilder();
    }

    @Test
    void noBrokerNodeIdRanges_usesLocalBrokerCount() {
        Service svc = builder.build(proxy(null), 3, NS);

        List<ServicePort> ports = svc.getSpec().getPorts();
        // 1 bootstrap + 3 brokers
        assertThat(ports).hasSize(4);
        assertThat(ports.get(0).getName()).isEqualTo("bootstrap");
        assertThat(ports.get(1).getName()).isEqualTo("broker-0");
        assertThat(ports.get(2).getName()).isEqualTo("broker-1");
        assertThat(ports.get(3).getName()).isEqualTo("broker-2");
    }

    @Test
    void withBrokerNodeIdRanges_usesRangesSum() {
        List<BrokerNodeIdRange> ranges = List.of(
                range("cluster-a", 0, 2),     // 3 nodes
                range("cluster-b", 1000, 1001) // 2 nodes
        );
        Service svc = builder.build(proxy(ranges), 1, NS);

        List<ServicePort> ports = svc.getSpec().getPorts();
        // 1 bootstrap + 5 brokers
        assertThat(ports).hasSize(6);
    }

    @Test
    void portNumbering() {
        Service svc = builder.build(proxy(null), 2, NS);

        List<ServicePort> ports = svc.getSpec().getPorts();
        assertThat(ports.get(0).getPort()).isEqualTo(CLIENT_PORT);         // bootstrap
        assertThat(ports.get(1).getPort()).isEqualTo(CLIENT_PORT + 1);     // broker-0
        assertThat(ports.get(2).getPort()).isEqualTo(CLIENT_PORT + 2);     // broker-1
    }

    @Test
    void serviceName_andSelector() {
        Service svc = builder.build(proxy(null), 1, NS);

        assertThat(svc.getMetadata().getName()).isEqualTo(PROXY_NAME);
        assertThat(svc.getMetadata().getNamespace()).isEqualTo(NS);
        // selector should be the ProxyDeploymentBuilder labels (app=PROXY_NAME)
        assertThat(svc.getSpec().getSelector()).containsValue(PROXY_NAME);
    }

    // --- helpers ---

    private KafkaProxy proxy(List<BrokerNodeIdRange> ranges) {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(PROXY_NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setClientPort(CLIENT_PORT);
        if (ranges != null) {
            spec.setBrokerNodeIdRanges(ranges);
        }
        p.setSpec(spec);
        return p;
    }

    private BrokerNodeIdRange range(String name, int start, int end) {
        BrokerNodeIdRange r = new BrokerNodeIdRange();
        r.setName(name);
        r.setStart(start);
        r.setEnd(end);
        return r;
    }
}
