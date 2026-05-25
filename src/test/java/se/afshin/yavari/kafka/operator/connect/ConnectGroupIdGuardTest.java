package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterRef;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpoint;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ConnectGroupIdGuardTest {

    private KubernetesClient client;
    private ConnectGroupIdGuard guard;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);
        guard = new ConnectGroupIdGuard();
        Field f = ConnectGroupIdGuard.class.getDeclaredField("client");
        f.setAccessible(true);
        f.set(guard, client);
    }

    private void setExisting(String namespace, List<KafkaConnect> list) {
        MixedOperation mixed = mock(MixedOperation.class);
        NonNamespaceOperation nso = mock(NonNamespaceOperation.class);
        KubernetesResourceList krl = mock(KubernetesResourceList.class);
        when(client.resources(KafkaConnect.class)).thenReturn(mixed);
        when(mixed.inNamespace(namespace)).thenReturn(nso);
        when(nso.list()).thenReturn(krl);
        when(krl.getItems()).thenReturn(list);
    }

    private KafkaConnect cr(String name, String groupId, String managedCluster) {
        KafkaConnect cr = new KafkaConnect();
        cr.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace("kafka").build());
        KafkaConnectSpec spec = new KafkaConnectSpec();
        spec.setGroupId(groupId);
        KafkaEndpoint ep = new KafkaEndpoint();
        KafkaClusterRef ref = new KafkaClusterRef();
        ref.setName(managedCluster);
        ep.setKafkaClusterRef(ref);
        spec.setKafkaClusterRef(ep);
        cr.setSpec(spec);
        return cr;
    }

    @Test
    void noConflictWhenSoleCr() {
        KafkaConnect me = cr("me", "g1", "my-kafka");
        setExisting("kafka", List.of(me));
        assertThat(guard.check(me)).isNull();
    }

    @Test
    void noConflictWhenSameGroupIdButDifferentCluster() {
        KafkaConnect me = cr("me", "g1", "kafka-a");
        KafkaConnect other = cr("other", "g1", "kafka-b");
        setExisting("kafka", List.of(me, other));
        assertThat(guard.check(me)).isNull();
    }

    @Test
    void conflictWhenSameGroupIdSameCluster() {
        KafkaConnect me = cr("me", "g1", "my-kafka");
        KafkaConnect other = cr("other", "g1", "my-kafka");
        setExisting("kafka", List.of(me, other));
        ConnectGroupIdGuard.Conflict c = guard.check(me);
        assertThat(c).isNotNull();
        assertThat(c.otherCrName()).isEqualTo("other");
    }

    @Test
    void defaultGroupIdAlsoCollides() {
        // Both default to connect-<name>; different metadata.name → different default groupId → no collision.
        KafkaConnect me = cr("me", null, "my-kafka");
        KafkaConnect other = cr("other", null, "my-kafka");
        setExisting("kafka", List.of(me, other));
        assertThat(guard.check(me)).isNull();
    }

    @Test
    void collisionFlaggedAcrossExplicitMatch() {
        // Explicit groupId on both sides — collision.
        KafkaConnect me = cr("me", "shared", "my-kafka");
        KafkaConnect other = cr("other", "shared", "my-kafka");
        setExisting("kafka", List.of(me, other));
        assertThat(guard.check(me)).isNotNull();
    }
}
