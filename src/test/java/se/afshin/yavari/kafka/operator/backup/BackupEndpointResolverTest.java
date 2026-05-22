package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterRef;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BackupEndpointResolverTest {

    private KafkaClusterRef ref(String name, String namespace) {
        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName(name);
        ref.setNamespace(namespace);
        return ref;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private BackupEndpointResolver resolverFor(KafkaCluster cluster, String bootstrap) {
        KubernetesClient client = mock(KubernetesClient.class);
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation nonNs = mock(NonNamespaceOperation.class);
        Resource resource = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(mixed);
        when(mixed.inNamespace("kafka")).thenReturn(nonNs);
        when(nonNs.withName("c")).thenReturn(resource);
        when(resource.get()).thenReturn(cluster);

        BrokerBootstrapResolver bootstrapResolver = mock(BrokerBootstrapResolver.class);
        when(bootstrapResolver.resolve("c", "kafka")).thenReturn(bootstrap);

        BackupEndpointResolver resolver = new BackupEndpointResolver();
        resolver.client = client;
        resolver.bootstrapResolver = bootstrapResolver;
        return resolver;
    }

    @Test
    void rejectsNullRef() {
        assertThatThrownBy(() -> new BackupEndpointResolver().resolve(null, "kafka"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCrossNamespaceRef() {
        assertThatThrownBy(() -> new BackupEndpointResolver().resolve(ref("c", "other"), "kafka"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cross-namespace");
    }

    @Test
    void resolvesManagedClusterToDirectBrokerBootstrap() {
        KafkaCluster cluster = new KafkaCluster();
        cluster.setSpec(new KafkaClusterSpec());

        ResolvedBackupEndpoint endpoint =
                resolverFor(cluster, "pool-headless.kafka.svc:9092").resolve(ref("c", null), "kafka");
        assertThat(endpoint.bootstrap()).isEqualTo("pool-headless.kafka.svc:9092");
        assertThat(endpoint.hasTls()).isFalse();
        assertThat(endpoint.hasApicurio()).isFalse();
    }

    @Test
    void resolvesMtlsSecretWhenClusterHasProxyMtls() {
        KafkaCluster cluster = new KafkaCluster();
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setProxyMtls(new KafkaProxyMtlsConfig());
        cluster.setSpec(spec);

        ResolvedBackupEndpoint endpoint =
                resolverFor(cluster, "broker:9092").resolve(ref("c", null), "kafka");
        assertThat(endpoint.hasTls()).isTrue();
        assertThat(endpoint.tlsSecretRef())
                .isEqualTo(KafkaProxyMtlsConfig.DEFAULT_ADMIN_CLIENT_CERT_SECRET);
    }
}
