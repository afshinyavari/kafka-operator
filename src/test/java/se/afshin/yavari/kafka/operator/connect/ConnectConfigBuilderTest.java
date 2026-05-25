package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectWorkerConfig;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectConfigBuilderTest {

    private final ConnectConfigBuilder builder = new ConnectConfigBuilder();

    private KafkaConnect cr() {
        KafkaConnect cr = new KafkaConnect();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-connect").withNamespace("kafka").build());
        cr.setSpec(new KafkaConnectSpec());
        return cr;
    }

    private ResolvedPluginSources noPlugins() {
        return new ResolvedPluginSources("/opt/kafka/connect-plugins/baked",
                List.of(), List.of(), List.of());
    }

    @Test
    void mtlsManagedClusterEmitsSslAndDisablesHostnameVerification() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint(
                "kafka-proxy.kafka.svc.cluster.local:9094",
                "kafka-operator-client-tls", null, null, null, false);

        String props = builder.build(cr(), ep, noPlugins());

        assertThat(props).contains("bootstrap.servers=kafka-proxy.kafka.svc.cluster.local:9094");
        assertThat(props).contains("group.id=connect-my-connect");
        assertThat(props).contains("config.storage.topic=connect-configs.my-connect");
        assertThat(props).contains("offset.storage.topic=connect-offsets.my-connect");
        assertThat(props).contains("status.storage.topic=connect-status.my-connect");
        assertThat(props).contains("security.protocol=SSL");
        assertThat(props).contains("producer.security.protocol=SSL");
        assertThat(props).contains("consumer.security.protocol=SSL");
        assertThat(props).contains("admin.security.protocol=SSL");
        assertThat(props).contains("ssl.endpoint.identification.algorithm=");
        assertThat(props).contains("listeners=http://0.0.0.0:8083");
    }

    @Test
    void plainPlaintextOmitsSecurityProtocol() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint(
                "b:9092", null, null, null, null, false);

        String props = builder.build(cr(), ep, noPlugins());

        assertThat(props).doesNotContain("security.protocol=");
        assertThat(props).doesNotContain("ssl.keystore.location=");
    }

    @Test
    void saslSslEmitsSaslMechanism() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint(
                "b:9093", "tls",
                new ResolvedKafkaEndpoint.Sasl("SCRAM-SHA-512", "creds"),
                null, null, false);

        String props = builder.build(cr(), ep, noPlugins());

        assertThat(props).contains("security.protocol=SASL_SSL");
        assertThat(props).contains("sasl.mechanism=SCRAM-SHA-512");
        assertThat(props).contains("producer.sasl.mechanism=SCRAM-SHA-512");
    }

    @Test
    void pluginPathFromResolvedSources() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        ResolvedPluginSources plugins = new ResolvedPluginSources(
                "/opt/kafka/connect-plugins/baked,/opt/kafka/connect-plugins/pvc,/opt/kafka/connect-plugins/cm-debezium",
                List.of(), List.of(), List.of());

        String props = builder.build(cr(), ep, plugins);

        assertThat(props).contains("plugin.path=/opt/kafka/connect-plugins/baked,/opt/kafka/connect-plugins/pvc,/opt/kafka/connect-plugins/cm-debezium");
    }

    @Test
    void groupIdOverrideHonored() {
        KafkaConnect cr = cr();
        cr.getSpec().setGroupId("custom-group");
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr, ep, noPlugins());
        assertThat(props).contains("group.id=custom-group");
        assertThat(props).doesNotContain("group.id=connect-my-connect");
    }

    @Test
    void converterDefaultsApplied() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        String props = builder.build(cr(), ep, noPlugins());
        assertThat(props).contains("key.converter=org.apache.kafka.connect.json.JsonConverter");
        assertThat(props).contains("value.converter=org.apache.kafka.connect.json.JsonConverter");
        assertThat(props).contains("key.converter.schemas.enable=false");
    }

    @Test
    void additionalPropertiesPassthroughButCannotOverrideClusterIdentity() {
        KafkaConnect cr = cr();
        KafkaConnectWorkerConfig w = new KafkaConnectWorkerConfig();
        w.setAdditionalProperties(Map.of(
                "rest.advertised.host.name", "my-host",
                "bootstrap.servers", "hijacked:9092",
                "config.storage.topic", "hijacked-config"));
        cr.getSpec().setWorker(w);
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr, ep, noPlugins());

        assertThat(props).contains("rest.advertised.host.name=my-host");
        // Cluster identity cannot be hijacked by additionalProperties.
        assertThat(props).contains("bootstrap.servers=b:9092");
        assertThat(props).doesNotContain("bootstrap.servers=hijacked");
        assertThat(props).contains("config.storage.topic=connect-configs.my-connect");
        assertThat(props).doesNotContain("config.storage.topic=hijacked-config");
    }

    @Test
    void internalReplicationFactorPropagates() {
        KafkaConnect cr = cr();
        KafkaConnectWorkerConfig w = new KafkaConnectWorkerConfig();
        w.setInternalReplicationFactor(5);
        cr.getSpec().setWorker(w);
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);

        String props = builder.build(cr, ep, noPlugins());

        assertThat(props).contains("config.storage.replication.factor=5");
        assertThat(props).contains("offset.storage.replication.factor=5");
        assertThat(props).contains("status.storage.replication.factor=5");
    }
}
