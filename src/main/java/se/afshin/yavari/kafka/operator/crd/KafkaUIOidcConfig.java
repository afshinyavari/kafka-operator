package se.afshin.yavari.kafka.operator.crd;

public class KafkaUIOidcConfig {
    private String issuerUrl;
    private String clientId;
    private KafkaUISecretKeyRef clientSecretRef;

    public String getIssuerUrl() { return issuerUrl; }
    public void setIssuerUrl(String issuerUrl) { this.issuerUrl = issuerUrl; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public KafkaUISecretKeyRef getClientSecretRef() { return clientSecretRef; }
    public void setClientSecretRef(KafkaUISecretKeyRef clientSecretRef) { this.clientSecretRef = clientSecretRef; }
}
