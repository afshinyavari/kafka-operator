package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyXmlFilterConfig {
    private boolean enabled = false;
    private String schemaTopic;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getSchemaTopic() { return schemaTopic; }
    public void setSchemaTopic(String schemaTopic) { this.schemaTopic = schemaTopic; }
}
