package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterApicurioSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterRef;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.Mm2Endpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2ExternalEndpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2SaslConfig;
import se.afshin.yavari.kafka.operator.crd.Mm2SchemaRegistryRef;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class Mm2EndpointResolverTest {

    private KubernetesClient client;
    private Mm2EndpointResolver resolver;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        resolver = new Mm2EndpointResolver();
        Field f = Mm2EndpointResolver.class.getDeclaredField("client");
        f.setAccessible(true);
        f.set(resolver, client);
    }

    private void registerManagedCluster(KafkaCluster cluster) {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation ns = mock(NonNamespaceOperation.class);
        Resource res = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(mixed);
        when(mixed.inNamespace(cluster.getMetadata().getNamespace())).thenReturn(ns);
        when(ns.withName(cluster.getMetadata().getName())).thenReturn(res);
        when(res.get()).thenReturn(cluster);
    }

    private KafkaCluster managedCluster(String name, String namespace, boolean withApicurio, boolean withMtls) {
        KafkaCluster c = new KafkaCluster();
        c.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(namespace).build());
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(List.of(new ClusterEntry()));
        spec.setProxy(new KafkaClusterProxySpec());
        if (withMtls) {
            spec.setProxyMtls(new KafkaProxyMtlsConfig());
        }
        if (withApicurio) {
            KafkaClusterApicurioSpec a = new KafkaClusterApicurioSpec();
            a.setRbacRef("kafka-rbac");
            spec.setApicurio(a);
        }
        c.setSpec(spec);
        return c;
    }

    @Test
    void managedRefResolvesToProxyBootstrap() {
        registerManagedCluster(managedCluster("my-cluster", "kafka", true, true));

        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName("my-cluster");
        Mm2Endpoint ep = new Mm2Endpoint();
        ep.setKafkaClusterRef(ref);

        ResolvedEndpoint r = resolver.resolve(ep, "kafka");

        assertThat(r.bootstrap()).isEqualTo("kafka-proxy.kafka.svc.cluster.local:9094");
        assertThat(r.tlsSecretRef()).isEqualTo("kafka-operator-client-tls");
        assertThat(r.hasSchemaRegistry()).isTrue();
        assertThat(r.schemaRegistryUrl()).contains("apicurio-rbac-proxy.kafka.svc.cluster.local");
    }

    @Test
    void managedRefWithoutMtlsHasNoTls() {
        registerManagedCluster(managedCluster("plain", "kafka", false, false));

        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName("plain");
        Mm2Endpoint ep = new Mm2Endpoint();
        ep.setKafkaClusterRef(ref);

        ResolvedEndpoint r = resolver.resolve(ep, "kafka");
        assertThat(r.hasTls()).isFalse();
        assertThat(r.hasSchemaRegistry()).isFalse();
    }

    @Test
    void missingManagedClusterThrows() {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation ns = mock(NonNamespaceOperation.class);
        Resource res = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(mixed);
        when(mixed.inNamespace("kafka")).thenReturn(ns);
        when(ns.withName("ghost")).thenReturn(res);
        when(res.get()).thenReturn(null);

        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName("ghost");
        Mm2Endpoint ep = new Mm2Endpoint();
        ep.setKafkaClusterRef(ref);

        assertThatThrownBy(() -> resolver.resolve(ep, "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void managedRefWithSchemaRegistryAuthSecret() {
        registerManagedCluster(managedCluster("my-cluster", "kafka", true, true));

        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName("my-cluster");
        Mm2Endpoint ep = new Mm2Endpoint();
        ep.setKafkaClusterRef(ref);
        ep.setSchemaRegistryAuthSecretRef("mm2-schema-registry-oauth");

        ResolvedEndpoint r = resolver.resolve(ep, "kafka");

        assertThat(r.schemaRegistryUrl()).contains("apicurio-rbac-proxy.kafka.svc.cluster.local");
        assertThat(r.schemaRegistryAuthSecretRef()).isEqualTo("mm2-schema-registry-oauth");
    }

    @Test
    void externalPassthrough() {
        Mm2Endpoint ep = new Mm2Endpoint();
        Mm2ExternalEndpoint ext = new Mm2ExternalEndpoint();
        ext.setBootstrap("broker.example.com:9093");
        ext.setTlsSecretRef("ext-tls");
        Mm2SaslConfig sasl = new Mm2SaslConfig();
        sasl.setMechanism("SCRAM-SHA-512");
        sasl.setSecretRef("ext-creds");
        ext.setSasl(sasl);
        Mm2SchemaRegistryRef sr = new Mm2SchemaRegistryRef();
        sr.setUrl("https://reg.external");
        sr.setAuthSecretRef("reg-auth");
        ext.setSchemaRegistry(sr);
        ep.setExternal(ext);

        ResolvedEndpoint r = resolver.resolve(ep, "kafka");

        assertThat(r.bootstrap()).isEqualTo("broker.example.com:9093");
        assertThat(r.tlsSecretRef()).isEqualTo("ext-tls");
        assertThat(r.hasSasl()).isTrue();
        assertThat(r.sasl().mechanism()).isEqualTo("SCRAM-SHA-512");
        assertThat(r.schemaRegistryUrl()).isEqualTo("https://reg.external");
        assertThat(r.schemaRegistryAuthSecretRef()).isEqualTo("reg-auth");
    }

    @Test
    void externalConfluentRejected() {
        Mm2Endpoint ep = new Mm2Endpoint();
        Mm2ExternalEndpoint ext = new Mm2ExternalEndpoint();
        ext.setBootstrap("b:9093");
        Mm2SchemaRegistryRef sr = new Mm2SchemaRegistryRef();
        sr.setUrl("https://reg");
        sr.setType(SchemaRegistryType.CONFLUENT);
        ext.setSchemaRegistry(sr);
        ep.setExternal(ext);

        assertThatThrownBy(() -> resolver.resolve(ep, "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CONFLUENT");
    }
}
