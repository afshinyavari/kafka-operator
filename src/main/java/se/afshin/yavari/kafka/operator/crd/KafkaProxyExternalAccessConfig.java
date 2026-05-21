package se.afshin.yavari.kafka.operator.crd;

import se.afshin.yavari.kafka.operator.externalaccess.ExternalAccessSpec;

public class KafkaProxyExternalAccessConfig implements ExternalAccessSpec {
    private ExternalAccessType type;
    private String advertisedHostTemplate;
    private KafkaProxyGatewayConfig gateway;
    private KafkaProxyIngressConfig ingress;

    @Override
    public ExternalAccessType getType() { return type; }
    public void setType(ExternalAccessType type) { this.type = type; }

    @Override
    public String getAdvertisedHostTemplate() { return advertisedHostTemplate; }
    public void setAdvertisedHostTemplate(String advertisedHostTemplate) { this.advertisedHostTemplate = advertisedHostTemplate; }

    public KafkaProxyGatewayConfig getGateway() { return gateway; }
    public void setGateway(KafkaProxyGatewayConfig gateway) { this.gateway = gateway; }

    public KafkaProxyIngressConfig getIngress() { return ingress; }
    public void setIngress(KafkaProxyIngressConfig ingress) { this.ingress = ingress; }
}
