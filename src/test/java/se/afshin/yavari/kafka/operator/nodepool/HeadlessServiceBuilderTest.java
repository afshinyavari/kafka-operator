package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class HeadlessServiceBuilderTest {

    private static final String NS = "kafka";
    private static final String CLUSTER = "my-cluster";
    private static final String POOL_NAME = "brokers-a";

    private HeadlessServiceBuilder builder;

    @BeforeEach
    void setup() throws Exception {
        builder = new HeadlessServiceBuilder();
        setField(builder, "mcsEnabled", false);
    }

    @Test
    void broker_addsBrokerPort() {
        Service svc = builder.build(pool(), NS, CLUSTER, false, true);

        assertThat(svc.getSpec().getPorts())
                .anyMatch(p -> "kafka".equals(p.getName()) && p.getPort() == 9092);
        assertThat(svc.getSpec().getPorts())
                .noneMatch(p -> "controller".equals(p.getName()));
    }

    @Test
    void controller_addsControllerPort() {
        Service svc = builder.build(pool(), NS, CLUSTER, true, false);

        assertThat(svc.getSpec().getPorts())
                .anyMatch(p -> "controller".equals(p.getName()) && p.getPort() == 9093);
        assertThat(svc.getSpec().getPorts())
                .noneMatch(p -> "kafka".equals(p.getName()));
    }

    @Test
    void combined_addsBothPorts() {
        Service svc = builder.build(pool(), NS, CLUSTER, true, true);

        assertThat(svc.getSpec().getPorts()).hasSize(2);
        assertThat(svc.getSpec().getPorts()).anyMatch(p -> p.getPort() == 9092);
        assertThat(svc.getSpec().getPorts()).anyMatch(p -> p.getPort() == 9093);
    }

    @Test
    void service_isHeadless() {
        Service svc = builder.build(pool(), NS, CLUSTER, false, true);

        assertThat(svc.getSpec().getClusterIP()).isEqualTo("None");
    }

    @Test
    void service_nameIsPoolNameHeadless() {
        Service svc = builder.build(pool(), NS, CLUSTER, false, true);

        assertThat(svc.getMetadata().getName()).isEqualTo(POOL_NAME + "-headless");
        assertThat(svc.getMetadata().getNamespace()).isEqualTo(NS);
    }

    @Test
    void serviceExport_mcsDisabled_returnsEmpty() {
        Optional<?> export = builder.buildServiceExport("my-svc", NS, pool());

        assertThat(export).isEmpty();
    }

    @Test
    void serviceExport_mcsEnabled_returnsExport() throws Exception {
        setField(builder, "mcsEnabled", true);

        Optional<?> export = builder.buildServiceExport("my-svc", NS, pool());

        assertThat(export).isPresent();
    }

    // --- Helpers ---

    private KafkaNodePool pool() {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        meta.setUid("pool-uid-123");
        pool.setMetadata(meta);
        pool.setSpec(new KafkaNodePoolSpec());
        return pool;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
