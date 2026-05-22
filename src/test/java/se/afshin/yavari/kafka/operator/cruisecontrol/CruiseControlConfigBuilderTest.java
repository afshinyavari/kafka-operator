package se.afshin.yavari.kafka.operator.cruisecontrol;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.CruiseControlApiSecurity;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlConfigBuilderTest {

    private final CruiseControlConfigBuilder builder = new CruiseControlConfigBuilder();

    @Test
    void rendersBootstrapAndKraftSettings() {
        String props = builder.build(new KafkaClusterCruiseControlSpec(),
                "brokers-a-headless.kafka.svc.cluster.local:9092", false);

        assertThat(props).contains("bootstrap.servers=brokers-a-headless.kafka.svc.cluster.local:9092");
        assertThat(props).contains("kafka.broker.failure.detection.enable=true");
        assertThat(props).contains("webserver.http.port=9090");
        // KafkaSampleStore requires explicit sample-store topic names.
        assertThat(props).contains("partition.metric.sample.store.topic=__KafkaCruiseControlPartitionMetricSamples");
        assertThat(props).contains("broker.metric.sample.store.topic=__KafkaCruiseControlModelTrainingSamples");
        // KRaft mode — never emit a ZooKeeper connection string.
        assertThat(props).doesNotContain("zookeeper");
        assertThat(props).doesNotContain("security.protocol=SSL");
    }

    @Test
    void addsSslPropsWhenMtls() {
        String props = builder.build(new KafkaClusterCruiseControlSpec(), "b:9092", true);

        assertThat(props).contains("security.protocol=SSL");
        assertThat(props).contains("ssl.keystore.location=/tmp/tls/INTERNAL/keystore.p12");
        assertThat(props).contains("ssl.truststore.location=/tmp/tls/INTERNAL/truststore.p12");
        assertThat(props).contains("ssl.endpoint.identification.algorithm=");
    }

    @Test
    void computedKeysOverrideUserConfig() {
        KafkaClusterCruiseControlSpec spec = new KafkaClusterCruiseControlSpec();
        spec.setConfig(Map.of("bootstrap.servers", "wrong:9092", "sampling.interval.ms", "60000"));

        String props = builder.build(spec, "right:9092", false);

        assertThat(props).contains("bootstrap.servers=right:9092");
        assertThat(props).doesNotContain("wrong:9092");
        // User-supplied keys the operator does not compute survive.
        assertThat(props).contains("sampling.interval.ms=60000");
    }

    @Test
    void rendersGoalsWhenProvided() {
        KafkaClusterCruiseControlSpec spec = new KafkaClusterCruiseControlSpec();
        spec.setGoals(List.of("GoalA", "GoalB"));

        String props = builder.build(spec, "b:9092", false);

        assertThat(props).contains("goals=GoalA,GoalB");
        assertThat(props).contains("default.goals=GoalA,GoalB");
    }

    @Test
    void enablesWebserverSecurityWhenApiSecurityOn() {
        KafkaClusterCruiseControlSpec spec = new KafkaClusterCruiseControlSpec();
        CruiseControlApiSecurity api = new CruiseControlApiSecurity();
        api.setEnabled(true);
        api.setBasicAuthSecretRef("cc-auth");
        spec.setApiSecurity(api);

        String props = builder.build(spec, "b:9092", false);

        assertThat(props).contains("webserver.security.enable=true");
        assertThat(props).contains("webserver.auth.credentials.file=/etc/cruise-control-auth/auth-credentials.properties");
    }
}
