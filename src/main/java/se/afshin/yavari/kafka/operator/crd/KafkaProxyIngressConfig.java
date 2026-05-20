package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyIngressConfig {
    private String ingressClassName;

    public String getIngressClassName() { return ingressClassName; }
    public void setIngressClassName(String ingressClassName) { this.ingressClassName = ingressClassName; }
}
