package se.afshin.yavari.kafka.operator.crd;

public class KafkaRbacGroup {
    private String name;
    private KafkaRbacKafkaAccess kafka;
    private KafkaRbacSchemaAccess schemaRegistry;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public KafkaRbacKafkaAccess getKafka() { return kafka; }
    public void setKafka(KafkaRbacKafkaAccess kafka) { this.kafka = kafka; }

    public KafkaRbacSchemaAccess getSchemaRegistry() { return schemaRegistry; }
    public void setSchemaRegistry(KafkaRbacSchemaAccess schemaRegistry) { this.schemaRegistry = schemaRegistry; }
}
