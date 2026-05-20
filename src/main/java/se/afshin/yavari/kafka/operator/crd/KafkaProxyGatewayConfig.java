package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyGatewayConfig {
    private String parentGatewayName;
    private String parentGatewayNamespace;
    private String sectionName;

    public String getParentGatewayName() { return parentGatewayName; }
    public void setParentGatewayName(String parentGatewayName) { this.parentGatewayName = parentGatewayName; }

    public String getParentGatewayNamespace() { return parentGatewayNamespace; }
    public void setParentGatewayNamespace(String parentGatewayNamespace) { this.parentGatewayNamespace = parentGatewayNamespace; }

    public String getSectionName() { return sectionName; }
    public void setSectionName(String sectionName) { this.sectionName = sectionName; }
}
