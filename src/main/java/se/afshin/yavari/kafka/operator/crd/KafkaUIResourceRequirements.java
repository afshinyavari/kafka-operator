package se.afshin.yavari.kafka.operator.crd;

import java.util.LinkedHashMap;
import java.util.Map;

public class KafkaUIResourceRequirements {
    private Map<String, String> requests = defaultRequests();
    private Map<String, String> limits = defaultLimits();

    public Map<String, String> getRequests() { return requests; }
    public void setRequests(Map<String, String> requests) { this.requests = requests; }

    public Map<String, String> getLimits() { return limits; }
    public void setLimits(Map<String, String> limits) { this.limits = limits; }

    private static Map<String, String> defaultRequests() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("cpu", "100m");
        m.put("memory", "256Mi");
        return m;
    }

    private static Map<String, String> defaultLimits() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("cpu", "500m");
        m.put("memory", "512Mi");
        return m;
    }
}
