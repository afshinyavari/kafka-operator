package se.afshin.yavari.kafka.operator.config;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServerPropertiesBuilderTest {

    private ServerPropertiesBuilder builder;

    @BeforeEach
    void setup() {
        KRaftConfigGenerator gen = new KRaftConfigGenerator();
        builder = new ServerPropertiesBuilder();
        // Inject manually (no CDI in plain unit test)
        try {
            var field = ServerPropertiesBuilder.class.getDeclaredField("kraftConfig");
            field.setAccessible(true);
            field.set(builder, gen);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void controllerOnlyProperties() {
        KafkaCluster cr = sampleCr();
        KafkaNodePoolSpec poolSpec = new KafkaNodePoolSpec();
        poolSpec.setRoles(List.of(NodeRole.CONTROLLER));
        poolSpec.setReplicas(1);

        Map<String, String> props = builder.buildProperties(
                cr, poolSpec, 0, 0,
                "10000@ctrl-a:9093,10001@ctrl-b:9093,10002@ctrl-c:9093",
                "ctrl-a.example.com:9093",
                null);

        assertThat(props.get("process.roles")).isEqualTo("controller");
        assertThat(props.get("node.id")).isEqualTo("10000");
        assertThat(props.get("controller.quorum.voters")).contains("10000@ctrl-a:9093");
        assertThat(props.get("listeners")).isEqualTo("CONTROLLER://${MY_POD_IP}:9093");
        assertThat(props).doesNotContainKey("advertised.listeners");
        assertThat(props).doesNotContainKey("inter.broker.listener.name");
    }

    @Test
    void brokerOnlyProperties() {
        KafkaCluster cr = sampleCr();
        KafkaNodePoolSpec poolSpec = new KafkaNodePoolSpec();
        poolSpec.setRoles(List.of(NodeRole.BROKER));
        poolSpec.setReplicas(3);

        Map<String, String> props = builder.buildProperties(
                cr, poolSpec, 1, 2,
                "10000@ctrl-a:9093,10001@ctrl-b:9093",
                null,
                "broker-b-2.example.com:9092");

        assertThat(props.get("process.roles")).isEqualTo("broker");
        assertThat(props.get("node.id")).isEqualTo("1002");  // clusterIndex=1, poolLocalIndex=2
        assertThat(props.get("inter.broker.listener.name")).isEqualTo("INTERNAL");
        assertThat(props.get("listeners")).isEqualTo("INTERNAL://0.0.0.0:9092");
        assertThat(props.get("listener.security.protocol.map")).isEqualTo("CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT");
    }

    @Test
    void combinedRoleProperties() {
        KafkaCluster cr = sampleCr();
        KafkaNodePoolSpec poolSpec = new KafkaNodePoolSpec();
        poolSpec.setRoles(List.of(NodeRole.CONTROLLER, NodeRole.BROKER));
        poolSpec.setReplicas(1);

        Map<String, String> props = builder.buildProperties(
                cr, poolSpec, 0, 0,
                "10000@ctrl-a:9093",
                "ctrl-a.example.com:9093",
                "broker-a.example.com:9092");

        assertThat(props.get("process.roles")).isEqualTo("controller,broker");
        assertThat(props.get("listeners")).contains("CONTROLLER://").contains("INTERNAL://");
    }

    @Test
    void poolConfigOverridesClusterConfig() {
        KafkaCluster cr = sampleCr();
        cr.getSpec().setConfig(Map.of("num.partitions", "3", "log.retention.hours", "168"));

        KafkaNodePoolSpec poolSpec = new KafkaNodePoolSpec();
        poolSpec.setRoles(List.of(NodeRole.BROKER));
        poolSpec.setConfig(Map.of("log.retention.hours", "24"));

        Map<String, String> props = builder.buildProperties(
                cr, poolSpec, 0, 0, "10000@x:9093", null, "b:9092");

        assertThat(props.get("log.retention.hours")).isEqualTo("24");
        assertThat(props.get("num.partitions")).isEqualTo("3");
    }

    @Test
    void toPropertiesStringFormat() {
        Map<String, String> props = Map.of("process.roles", "broker", "node.id", "5");
        String output = builder.toPropertiesString(props);
        assertThat(output).contains("process.roles=broker\n");
        assertThat(output).contains("node.id=5\n");
    }

    // Helpers

    private KafkaCluster sampleCr() {
        KafkaCluster cr = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("my-kafka");
        meta.setUid("550e8400e29b41d4a716446655440000");
        cr.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(List.of(
                cluster("A", "ctrl-a.example.com:9093"),
                cluster("B", "ctrl-b.example.com:9093"),
                cluster("C", "ctrl-c.example.com:9093")
        ));
        cr.setSpec(spec);
        return cr;
    }

    private ClusterEntry cluster(String id, String addr) {
        ClusterEntry e = new ClusterEntry();
        e.setId(id);
        e.setControllerAdvertisedAddress(addr);
        return e;
    }
}
