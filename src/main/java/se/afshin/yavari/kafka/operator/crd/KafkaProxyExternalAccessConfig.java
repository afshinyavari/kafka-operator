package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyExternalAccessConfig {
    private ExternalAccessType type;
    private String advertisedHostTemplate;
    private KafkaProxyGatewayConfig gateway;
    private KafkaProxyIngressConfig ingress;

    public ExternalAccessType getType() { return type; }
    public void setType(ExternalAccessType type) { this.type = type; }

    public String getAdvertisedHostTemplate() { return advertisedHostTemplate; }
    public void setAdvertisedHostTemplate(String advertisedHostTemplate) { this.advertisedHostTemplate = advertisedHostTemplate; }

    public KafkaProxyGatewayConfig getGateway() { return gateway; }
    public void setGateway(KafkaProxyGatewayConfig gateway) { this.gateway = gateway; }

    public KafkaProxyIngressConfig getIngress() { return ingress; }
    public void setIngress(KafkaProxyIngressConfig ingress) { this.ingress = ingress; }
}
