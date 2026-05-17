package se.afshin.yavari.kafka.operator.rolling;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class CrossClusterRollCoordinator {

    private static final Logger LOG = Logger.getLogger(CrossClusterRollCoordinator.class);

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /**
     * Returns true if this cluster is the next one allowed to roll controllers according to
     * spec.clusterRollOrder. Always returns true when clusterRollOrder is not set.
     *
     * Each preceding cluster must report upgradePhase=IDLE via its operatorAddress endpoint.
     * If operatorAddress is missing for a preceding cluster, that cluster is skipped (warn + fail-open).
     */
    public boolean isMyTurnToRoll(KafkaClusterSpec spec, String localClusterId) {
        List<String> order = spec.getClusterRollOrder();
        if (order == null || order.isEmpty()) return true;

        int myIndex = order.indexOf(localClusterId);
        if (myIndex <= 0) return true;

        Map<String, ClusterEntry> byId = spec.getClusters().stream()
                .collect(Collectors.toMap(ClusterEntry::getId, e -> e));

        for (int i = 0; i < myIndex; i++) {
            String precedingId = order.get(i);
            ClusterEntry entry = byId.get(precedingId);
            if (entry == null || entry.getOperatorAddress() == null) {
                LOG.warnf("clusterRollOrder: no operatorAddress for '%s' — skipping check", precedingId);
                continue;
            }
            String phase = fetchPhase(entry.getOperatorAddress(), precedingId);
            if (!"IDLE".equals(phase)) {
                LOG.infof("clusterRollOrder: '%s' is %s — not my turn yet", precedingId, phase);
                return false;
            }
        }
        return true;
    }

    private String fetchPhase(String address, String clusterId) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + address + "/operator/upgrade-phase"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            String body = http.send(req, HttpResponse.BodyHandlers.ofString()).body();
            // minimal parse to avoid pulling in a JSON library
            int idx = body.indexOf("\"upgradePhase\"");
            if (idx < 0) return "UNKNOWN";
            int colon = body.indexOf(':', idx);
            int start = body.indexOf('"', colon) + 1;
            return body.substring(start, body.indexOf('"', start));
        } catch (Exception e) {
            LOG.warnf("Cannot reach operator at %s (%s) — treating as IDLE: %s",
                    address, clusterId, e.getMessage());
            return "IDLE";
        }
    }
}
