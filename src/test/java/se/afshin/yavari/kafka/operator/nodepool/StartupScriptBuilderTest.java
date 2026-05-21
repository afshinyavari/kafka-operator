package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StartupScriptBuilderTest {

    private static final String NS = "kafka";
    private static final String POOL_NAME = "brokers-a";

    private StartupScriptBuilder builder;

    @BeforeEach
    void setup() throws Exception {
        builder = new StartupScriptBuilder();
        injectField(builder, "mcsEnabled", false);
    }

    @Test
    void controllerOnly_hasMyPodIpSed_noNodeId() {
        KafkaNodePool pool = pool(List.of(NodeRole.CONTROLLER));
        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).contains("MY_POD_IP");
        assertThat(script).doesNotContain("NODE_ID=$(");
        assertThat(script).doesNotContain("ADVERTISED_ADDR=");
    }

    @Test
    void controllerOnly_withControllerTls_hasPkcs12Block() {
        KafkaNodePool pool = pool(List.of(NodeRole.CONTROLLER));
        KafkaListenerTlsConfig ctrlTls = new KafkaListenerTlsConfig();

        String script = builder.build(pool, 0, NS, false, List.of(), ctrlTls, false);

        assertThat(script).contains("openssl pkcs12");
        assertThat(script).contains("/tmp/tls/CONTROLLER");
    }

    @Test
    void controllerOnly_noTls_noPkcs12Block() {
        KafkaNodePool pool = pool(List.of(NodeRole.CONTROLLER));

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).doesNotContain("openssl pkcs12");
        assertThat(script).doesNotContain("/tmp/tls/");
    }

    @Test
    void broker_mcsEnabled_usesClustersetAddress() throws Exception {
        injectField(builder, "mcsEnabled", true);
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).contains("clusterset.local");
        assertThat(script).doesNotContain("svc.cluster.local");
    }

    @Test
    void broker_mcsDisabled_usesPodFqdn() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).contains("svc.cluster.local");
        assertThat(script).contains("${POD_NAME}");
        assertThat(script).doesNotContain("clusterset.local");
    }

    @Test
    void broker_internalListenerWithTls_usesFqdnAndHasPkcs12() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));
        KafkaListenerTlsConfig tls = new KafkaListenerTlsConfig();
        KafkaListenerSpec l = listener("SECURE", 9095, tls);

        String script = builder.build(pool, 0, NS, false, List.of(l), null, false);

        // internal listener: address derived from ADVERTISED_ADDR minus port
        assertThat(script).contains("SECURE_ADDR=\"${ADVERTISED_ADDR%:*}:9095\"");
        // PKCS12 block for this listener
        assertThat(script).contains("openssl pkcs12");
        assertThat(script).contains("/tmp/tls/SECURE");
    }

    @Test
    void broker_withRack_hasBrokerRackSed() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));
        pool.getSpec().setRackTopologyKey("topology.kubernetes.io/zone");

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).contains("${BROKER_RACK}");
    }

    @Test
    void broker_withoutRack_noBrokerRackSed() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).doesNotContain("BROKER_RACK");
    }

    @Test
    void metricsEnabled_hasJmxExportLine() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        String script = builder.build(pool, 0, NS, true, List.of(), null, false);

        assertThat(script).contains("jmx-exporter.jar");
        assertThat(script).contains("KAFKA_OPTS");
    }

    @Test
    void metricsDisabled_noJmxExportLine() {
        KafkaNodePool pool = pool(List.of(NodeRole.BROKER));

        String script = builder.build(pool, 0, NS, false, List.of(), null, false);

        assertThat(script).doesNotContain("jmx-exporter.jar");
    }

    // --- helpers ---

    private KafkaNodePool pool(List<NodeRole> roles) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        meta.setNamespace(NS);
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(roles);
        spec.setReplicas(1);
        pool.setSpec(spec);
        return pool;
    }

    private KafkaListenerSpec listener(String name, int port, KafkaListenerTlsConfig tls) {
        KafkaListenerSpec l = new KafkaListenerSpec();
        l.setName(name);
        l.setPort(port);
        l.setTls(tls);
        return l;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
