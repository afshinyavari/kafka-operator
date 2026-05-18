package se.afshin.yavari.kafka.operator.crd;

import java.util.List;

public class KafkaProxySchemaRegistryConfig {
    private boolean enabled = false;
    private String schemaType = "JSON_SCHEMA";
    private List<String> topics = List.of();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getSchemaType() { return schemaType; }
    public void setSchemaType(String schemaType) { this.schemaType = schemaType; }

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }
}
