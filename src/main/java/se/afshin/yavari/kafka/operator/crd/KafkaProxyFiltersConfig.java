package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyFiltersConfig {
    private KafkaProxyXmlFilterConfig xmlValidation = new KafkaProxyXmlFilterConfig();
    private KafkaProxySchemaRegistryConfig schemaRegistry = new KafkaProxySchemaRegistryConfig();

    public KafkaProxyXmlFilterConfig getXmlValidation() { return xmlValidation; }
    public void setXmlValidation(KafkaProxyXmlFilterConfig xmlValidation) { this.xmlValidation = xmlValidation; }

    public KafkaProxySchemaRegistryConfig getSchemaRegistry() { return schemaRegistry; }
    public void setSchemaRegistry(KafkaProxySchemaRegistryConfig schemaRegistry) { this.schemaRegistry = schemaRegistry; }
}
