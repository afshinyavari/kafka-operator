package se.afshin.yavari.kafka.operator.infra;

import se.afshin.yavari.kafka.operator.crd.Condition;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Sets or updates a Kubernetes-style {@link Condition} on a status. Preserves
 * {@code lastTransitionTime} when status hasn't actually changed (per the convention).
 */
public final class ConditionUtil {

    public static final String AVAILABLE = "Available";
    public static final String PROGRESSING = "Progressing";
    public static final String DEGRADED = "Degraded";

    public static final String TRUE = "True";
    public static final String FALSE = "False";
    public static final String UNKNOWN = "Unknown";

    private ConditionUtil() {}

    public static List<Condition> set(List<Condition> existing, String type, String status,
                                      String reason, String message, Long observedGeneration) {
        List<Condition> out = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        for (int i = 0; i < out.size(); i++) {
            Condition c = out.get(i);
            if (type.equals(c.getType())) {
                if (Objects.equals(status, c.getStatus())) {
                    // No transition — keep lastTransitionTime, refresh reason/message/observedGen.
                    out.set(i, new Condition(type, status, reason, message,
                            c.getLastTransitionTime(), observedGeneration));
                } else {
                    out.set(i, new Condition(type, status, reason, message,
                            Instant.now().toString(), observedGeneration));
                }
                return out;
            }
        }
        out.add(new Condition(type, status, reason, message,
                Instant.now().toString(), observedGeneration));
        return out;
    }
}
