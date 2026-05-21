package se.afshin.yavari.kafka.operator.mm2;

import java.util.Map;

/** Standard label set for all MM2-owned resources. */
public final class Mm2Labels {

    private Mm2Labels() {}

    public static Map<String, String> labels(String mm2Name) {
        return Map.of(
                "app", "mirrormaker2",
                "app.instance", mm2Name,
                "app.managed-by", "kafka-operator"
        );
    }
}
