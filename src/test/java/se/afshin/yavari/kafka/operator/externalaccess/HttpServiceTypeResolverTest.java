package se.afshin.yavari.kafka.operator.externalaccess;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;

import static org.assertj.core.api.Assertions.assertThat;

class HttpServiceTypeResolverTest {

    @Test
    void nullConfig_returnsClusterIp() {
        assertThat(HttpServiceTypeResolver.resolve(null)).isEqualTo("ClusterIP");
    }

    @Test
    void nullType_returnsClusterIp() {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(null);
        assertThat(HttpServiceTypeResolver.resolve(ea)).isEqualTo("ClusterIP");
    }

    @Test
    void nodePort_returnsNodePort() {
        assertThat(HttpServiceTypeResolver.resolve(ea(ExternalAccessType.NODEPORT))).isEqualTo("NodePort");
    }

    @Test
    void loadBalancer_returnsLoadBalancer() {
        assertThat(HttpServiceTypeResolver.resolve(ea(ExternalAccessType.LOADBALANCER))).isEqualTo("LoadBalancer");
    }

    @Test
    void gateway_returnsClusterIp() {
        assertThat(HttpServiceTypeResolver.resolve(ea(ExternalAccessType.GATEWAY))).isEqualTo("ClusterIP");
    }

    @Test
    void ingress_returnsClusterIp() {
        assertThat(HttpServiceTypeResolver.resolve(ea(ExternalAccessType.INGRESS))).isEqualTo("ClusterIP");
    }

    private HttpExternalAccessConfig ea(ExternalAccessType type) {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(type);
        return ea;
    }
}
