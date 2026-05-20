package se.afshin.yavari.kafka.operator.topic;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class BrokerBootstrapResolverTest {

    private static final String NS = "kafka";
    private static final String CLUSTER = "my-cluster";

    private KubernetesClient client;
    private BrokerBootstrapResolver resolver;
    private FilterWatchListDeletable labelled;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        MixedOperation poolsOp = mock(MixedOperation.class);
        NonNamespaceOperation nsOp = mock(NonNamespaceOperation.class);
        labelled = mock(FilterWatchListDeletable.class);
        when(client.resources(KafkaNodePool.class)).thenReturn(poolsOp);
        when(poolsOp.inNamespace(NS)).thenReturn(nsOp);
        when(nsOp.withLabel(anyString(), anyString())).thenReturn(labelled);

        resolver = new BrokerBootstrapResolver();
        var field = BrokerBootstrapResolver.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(resolver, client);
    }

    @Test
    void picksAlphabeticallyFirstBrokerPool() {
        stubPools(List.of(
                pool("zeta-pool", NodeRole.BROKER),
                pool("alpha-pool", NodeRole.BROKER),
                pool("controller-pool", NodeRole.CONTROLLER)));

        String bootstrap = resolver.resolve(CLUSTER, NS);

        assertThat(bootstrap).isEqualTo("alpha-pool-headless." + NS + ".svc.cluster.local:9092");
    }

    @Test
    void ignoresControllerOnlyPools() {
        stubPools(List.of(
                pool("controller-pool", NodeRole.CONTROLLER),
                pool("broker-pool", NodeRole.BROKER)));

        assertThat(resolver.resolve(CLUSTER, NS))
                .isEqualTo("broker-pool-headless." + NS + ".svc.cluster.local:9092");
    }

    @Test
    void throwsWhenNoBrokerPoolExists() {
        stubPools(List.of(pool("controller-pool", NodeRole.CONTROLLER)));

        assertThatThrownBy(() -> resolver.resolve(CLUSTER, NS))
                .isInstanceOf(BrokerBootstrapResolver.BrokerPoolNotFoundException.class)
                .hasMessageContaining(CLUSTER);
    }

    @Test
    void throwsWhenNoPoolsAtAll() {
        stubPools(List.of());

        assertThatThrownBy(() -> resolver.resolve(CLUSTER, NS))
                .isInstanceOf(BrokerBootstrapResolver.BrokerPoolNotFoundException.class);
    }

    private void stubPools(List<KafkaNodePool> pools) {
        KubernetesResourceList list = mock(KubernetesResourceList.class);
        when(list.getItems()).thenReturn(pools);
        when(labelled.list()).thenReturn(list);
    }

    private KafkaNodePool pool(String name, NodeRole role) {
        KafkaNodePool p = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(name);
        p.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(List.of(role));
        p.setSpec(spec);
        return p;
    }
}
