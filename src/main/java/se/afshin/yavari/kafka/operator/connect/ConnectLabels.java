package se.afshin.yavari.kafka.operator.connect;

import java.util.Map;

/** Standard label set for KafkaConnect-owned resources (Deployment, Service, ConfigMap,
 *  ServiceMonitor). Used as both label and selector. */
public final class ConnectLabels {

    private ConnectLabels() {}

    public static Map<String, String> labels(String connectName) {
        return Map.of(
                "app", "kafka-connect",
                "app.instance", connectName,
                "app.managed-by", "kafka-operator"
        );
    }
}
