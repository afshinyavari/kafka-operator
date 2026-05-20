package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyExternalAccessConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ExternalAccessResolverTest {

    private static final String NS = "kafka";
    private static final String NAME = "kafka-proxy";

    private ExternalAccessResolver resolver;
    private KubernetesClient client;
    private ServiceResource namedSvc;

    @BeforeEach
    void setup() {
        resolver = new ExternalAccessResolver();
        client = mock(KubernetesClient.class);

        MixedOperation svcOp = mock(MixedOperation.class);
        NonNamespaceOperation nsSvcOp = mock(NonNamespaceOperation.class);
        namedSvc = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.withName(anyString())).thenReturn(namedSvc);
    }

    @Test
    void noExternalAccess_returnsInternal() {
        KafkaProxy p = proxy(null);

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.isInternal()).isTrue();
        assertThat(r.advertisedHost()).isNull();
    }

    @Test
    void loadBalancer_resolvesIpFromServiceStatus() {
        KafkaProxy p = proxy(ext(ExternalAccessType.LOADBALANCER, null));
        when(namedSvc.get()).thenReturn(svcWithLb("192.0.2.10", null));

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.isInternal()).isFalse();
        assertThat(r.isPending()).isFalse();
        assertThat(r.advertisedHost()).isEqualTo("192.0.2.10");
        assertThat(r.type()).isEqualTo(ExternalAccessType.LOADBALANCER);
    }

    @Test
    void loadBalancer_prefersHostnameOverIp() {
        KafkaProxy p = proxy(ext(ExternalAccessType.LOADBALANCER, null));
        when(namedSvc.get()).thenReturn(svcWithLb("192.0.2.10", "lb-1.elb.aws.com"));

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.advertisedHost()).isEqualTo("lb-1.elb.aws.com");
    }

    @Test
    void loadBalancer_pendingWhenIngressEmpty() {
        KafkaProxy p = proxy(ext(ExternalAccessType.LOADBALANCER, null));
        when(namedSvc.get()).thenReturn(svcWithLb(null, null));

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.isPending()).isTrue();
        assertThat(r.advertisedHost()).isNull();
    }

    @Test
    void loadBalancer_pendingWhenServiceMissing() {
        KafkaProxy p = proxy(ext(ExternalAccessType.LOADBALANCER, null));
        when(namedSvc.get()).thenReturn(null);

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.isPending()).isTrue();
    }

    @Test
    void loadBalancer_userSuppliedTemplateOverridesAutoResolve() {
        KafkaProxy p = proxy(ext(ExternalAccessType.LOADBALANCER, "kafka.example.com"));

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.advertisedHost()).isEqualTo("kafka.example.com");
    }

    @Test
    void gateway_substitutesClusterIdLowercase() {
        KafkaProxy p = proxy(ext(ExternalAccessType.GATEWAY, "${clusterId}.kafka.example.com"));

        ExternalAccessResolution r = resolver.resolve(p, "A", NS, client);

        assertThat(r.advertisedHost()).isEqualTo("a.kafka.example.com");
        assertThat(r.type()).isEqualTo(ExternalAccessType.GATEWAY);
    }

    @Test
    void ingress_substitutesClusterIdLowercase() {
        KafkaProxy p = proxy(ext(ExternalAccessType.INGRESS, "${clusterId}.kafka.example.com"));

        ExternalAccessResolution r = resolver.resolve(p, "B", NS, client);

        assertThat(r.advertisedHost()).isEqualTo("b.kafka.example.com");
    }

    @Test
    void gateway_missingTemplateThrows() {
        KafkaProxy p = proxy(ext(ExternalAccessType.GATEWAY, null));

        assertThatThrownBy(() -> resolver.resolve(p, "A", NS, client))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("advertisedHostTemplate is required");
    }

    @Test
    void ingress_missingTemplateThrows() {
        KafkaProxy p = proxy(ext(ExternalAccessType.INGRESS, ""));

        assertThatThrownBy(() -> resolver.resolve(p, "A", NS, client))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void substitute_noPlaceholderIsPassThrough() {
        assertThat(ExternalAccessResolver.substitute("kafka.example.com", "A"))
                .isEqualTo("kafka.example.com");
    }

    // --- helpers ---

    private KafkaProxy proxy(KafkaProxyExternalAccessConfig ea) {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setExternalAccess(ea);
        p.setSpec(spec);
        return p;
    }

    private KafkaProxyExternalAccessConfig ext(ExternalAccessType type, String template) {
        KafkaProxyExternalAccessConfig ea = new KafkaProxyExternalAccessConfig();
        ea.setType(type);
        ea.setAdvertisedHostTemplate(template);
        return ea;
    }

    private Service svcWithLb(String ip, String hostname) {
        Service svc = new Service();
        ServiceStatus status = new ServiceStatus();
        LoadBalancerStatus lb = new LoadBalancerStatus();
        if (ip != null || hostname != null) {
            LoadBalancerIngress ing = new LoadBalancerIngressBuilder()
                    .withIp(ip)
                    .withHostname(hostname)
                    .build();
            lb.setIngress(List.of(ing));
        } else {
            lb.setIngress(List.of());
        }
        status.setLoadBalancer(lb);
        svc.setStatus(status);
        return svc;
    }
}
