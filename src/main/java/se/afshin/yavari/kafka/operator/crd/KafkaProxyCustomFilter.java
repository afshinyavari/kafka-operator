package se.afshin.yavari.kafka.operator.crd;

import java.util.Map;

public class KafkaProxyCustomFilter {
    private String name;
    private String type;
    private Map<String, Object> config;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Map<String, Object> getConfig() { return config; }
    public void setConfig(Map<String, Object> config) { this.config = config; }
}
