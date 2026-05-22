package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
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

    @Test
    void defaultBuild_isClusterIp() {
        Service svc = builder.build(proxy(null), 1, NS);

        // No spec.type set → defaults to ClusterIP downstream.
        assertThat(svc.getSpec().getType()).isNull();
    }

    @Test
    void loadBalancer_setsServiceTypeAndKeepsPerBrokerPorts() {
        Service svc = builder.build(proxy(null), 3, NS,
                ExternalAccessResolution.resolved(ExternalAccessType.LOADBALANCER, "10.0.0.1"));

        assertThat(svc.getSpec().getType()).isEqualTo("LoadBalancer");
        // bootstrap + 3 broker ports (portIdentifiesNode mode kept for LB).
        assertThat(svc.getSpec().getPorts()).hasSize(4);
    }

    @Test
    void gateway_singlePort_clusterIp() {
        Service svc = builder.build(proxy(null), 3, NS,
                ExternalAccessResolution.resolved(ExternalAccessType.GATEWAY, "a.kafka.example.com"));

        // sniHostIdentifiesNode → one bootstrap port only.
        assertThat(svc.getSpec().getPorts()).hasSize(1);
        assertThat(svc.getSpec().getPorts().get(0).getName()).isEqualTo("bootstrap");
        // Gateway routes to the proxy via ClusterIP — no LoadBalancer/NodePort type.
        assertThat(svc.getSpec().getType()).isNull();
    }

    @Test
    void ingress_singlePort_clusterIp() {
        Service svc = builder.build(proxy(null), 3, NS,
                ExternalAccessResolution.resolved(ExternalAccessType.INGRESS, "a.kafka.example.com"));

        assertThat(svc.getSpec().getPorts()).hasSize(1);
        assertThat(svc.getSpec().getType()).isNull();
    }

    @Test
    void pendingLb_stillSetsLoadBalancerType() {
        // Even before the LB ingress is allocated, we want Type=LoadBalancer so the cloud
        // controller starts provisioning. The reconciler reschedules until the ingress shows up.
        Service svc = builder.build(proxy(null), 1, NS,
                ExternalAccessResolution.pending(ExternalAccessType.LOADBALANCER));

        assertThat(svc.getSpec().getType()).isEqualTo("LoadBalancer");
    }

    @Test
    void mainService_carriesNoMetricsPort() {
        // The metrics port belongs on the dedicated kafka-proxy-metrics ClusterIP Service
        // (built by MetricsResources), never the main Service — which may be a LoadBalancer.
        Service svc = builder.build(proxy(null), 3, NS);
        assertThat(svc.getSpec().getPorts()).extracting(ServicePort::getName)
                .doesNotContain("metrics");

        Service lb = builder.build(proxy(null), 3, NS,
                ExternalAccessResolution.resolved(ExternalAccessType.LOADBALANCER, "10.0.0.1"));
        assertThat(lb.getSpec().getPorts()).extracting(ServicePort::getName)
                .doesNotContain("metrics");
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
