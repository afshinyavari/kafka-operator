package se.afshin.yavari.kafka.operator.connect;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorDriftDetectorTest {

    private final ConnectorDriftDetector d = new ConnectorDriftDetector();

    @Test
    void identicalNonSensitiveNoDrift() {
        Map<String, String> a = Map.of("connector.class", "X", "topic", "t");
        assertThat(d.drifted(a, a)).isFalse();
    }

    @Test
    void differingNonSensitiveDrift() {
        assertThat(d.drifted(
                Map.of("topic", "a"),
                Map.of("topic", "b"))).isTrue();
    }

    @Test
    void maskedSensitiveAssumedEqual() {
        assertThat(d.drifted(
                Map.of("database.password", "hunter2"),
                Map.of("database.password", "********"))).isFalse();
    }

    @Test
    void sensitivePlaceholderRoundTripCompared() {
        assertThat(d.drifted(
                Map.of("database.password", "${file:/x:pw}"),
                Map.of("database.password", "${file:/x:pw}"))).isFalse();
        assertThat(d.drifted(
                Map.of("database.password", "${file:/x:pw}"),
                Map.of("database.password", "${file:/y:pw}"))).isTrue();
    }

    @Test
    void extraKeyInActualMeansDriftOperatorOwns() {
        assertThat(d.drifted(
                Map.of("a", "1"),
                Map.of("a", "1", "b", "2"))).isTrue();
    }

    @Test
    void missingKeyInActualIsDrift() {
        assertThat(d.drifted(
                Map.of("a", "1", "b", "2"),
                Map.of("a", "1"))).isTrue();
    }

    @Test
    void sensitiveKeysIdentifiedBySuffix() {
        assertThat(ConnectorDriftDetector.isSensitive("database.password")).isTrue();
        assertThat(ConnectorDriftDetector.isSensitive("auth.token")).isTrue();
        assertThat(ConnectorDriftDetector.isSensitive("api.key")).isTrue();
        assertThat(ConnectorDriftDetector.isSensitive("aws.credentials")).isTrue();
        assertThat(ConnectorDriftDetector.isSensitive("DATABASE.PASSWORD")).isTrue();
        assertThat(ConnectorDriftDetector.isSensitive("topic")).isFalse();
        assertThat(ConnectorDriftDetector.isSensitive("connector.class")).isFalse();
    }
}
