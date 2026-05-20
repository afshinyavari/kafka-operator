package se.afshin.yavari.kafka.operator.ui;

import java.util.Map;

final class UILabels {
    private UILabels() {}

    static Map<String, String> labels(String name) {
        return Map.of(
                "app", "kafka-ui",
                "app.instance", name,
                "app.managed-by", "kafka-operator"
        );
    }
}
