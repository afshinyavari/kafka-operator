package se.afshin.yavari.kafka.operator.rolling;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CrossClusterRollCoordinatorTest {

    private CrossClusterRollCoordinator coordinator;
    private HttpServer httpServer;
    private int serverPort;

    @BeforeEach
    void setup() throws Exception {
        coordinator = new CrossClusterRollCoordinator();
        httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverPort = httpServer.getAddress().getPort();
        httpServer.start();
    }

    @AfterEach
    void teardown() {
        if (httpServer != null) httpServer.stop(0);
    }

    @Test
    void noRollOrder_alwaysMyTurn() {
        KafkaClusterSpec spec = spec(List.of(entry("a", null)), null);
        assertThat(coordinator.isMyTurnToRoll(spec, "a")).isTrue();
    }

    @Test
    void emptyRollOrder_alwaysMyTurn() {
        KafkaClusterSpec spec = spec(List.of(entry("a", null)), List.of());
        assertThat(coordinator.isMyTurnToRoll(spec, "a")).isTrue();
    }

    @Test
    void firstInOrder_alwaysMyTurn() {
        // "a" is first — no preceding clusters to check
        KafkaClusterSpec spec = spec(
                List.of(entry("a", null), entry("b", null)),
                List.of("a", "b"));
        assertThat(coordinator.isMyTurnToRoll(spec, "a")).isTrue();
    }

    @Test
    void precedingClusterIdle_myTurn() throws Exception {
        servePhase("IDLE");
        KafkaClusterSpec spec = spec(
                List.of(entry("a", "127.0.0.1:" + serverPort), entry("b", null)),
                List.of("a", "b"));

        assertThat(coordinator.isMyTurnToRoll(spec, "b")).isTrue();
    }

    @Test
    void precedingClusterRolling_notMyTurn() throws Exception {
        servePhase("ROLLING");
        KafkaClusterSpec spec = spec(
                List.of(entry("a", "127.0.0.1:" + serverPort), entry("b", null)),
                List.of("a", "b"));

        assertThat(coordinator.isMyTurnToRoll(spec, "b")).isFalse();
    }

    @Test
    void precedingClusterMissingAddress_skippedAndMyTurn() {
        // "a" has no operatorAddress — should warn and skip → my turn
        KafkaClusterSpec spec = spec(
                List.of(entry("a", null), entry("b", null)),
                List.of("a", "b"));

        assertThat(coordinator.isMyTurnToRoll(spec, "b")).isTrue();
    }

    @Test
    void httpUnreachable_treatsAsIdle() {
        // Use a port that is not listening
        KafkaClusterSpec spec = spec(
                List.of(entry("a", "127.0.0.1:19999"), entry("b", null)),
                List.of("a", "b"));

        // Unreachable → treated as IDLE → my turn
        assertThat(coordinator.isMyTurnToRoll(spec, "b")).isTrue();
    }

    // --- helpers ---

    private void servePhase(String phase) {
        String body = "{\"upgradePhase\":\"" + phase + "\"}";
        httpServer.createContext("/operator/upgrade-phase", exchange -> {
            byte[] resp = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            exchange.getResponseBody().write(resp);
            exchange.getResponseBody().close();
        });
    }

    private KafkaClusterSpec spec(List<ClusterEntry> clusters, List<String> rollOrder) {
        KafkaClusterSpec s = new KafkaClusterSpec();
        s.setClusters(clusters);
        s.setClusterRollOrder(rollOrder);
        return s;
    }

    private ClusterEntry entry(String id, String operatorAddress) {
        ClusterEntry e = new ClusterEntry();
        e.setId(id);
        e.setControllerAdvertisedAddress("ctrl:9093");
        e.setOperatorAddress(operatorAddress);
        return e;
    }
}
