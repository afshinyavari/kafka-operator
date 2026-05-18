package se.afshin.yavari.kafka.operator.crd;

public class KafkaProxyTlsConfig {
    private String proxyKeySecretRef;
    private String clientCaSecretRef;

    public String getProxyKeySecretRef() { return proxyKeySecretRef; }
    public void setProxyKeySecretRef(String proxyKeySecretRef) { this.proxyKeySecretRef = proxyKeySecretRef; }

    public String getClientCaSecretRef() { return clientCaSecretRef; }
    public void setClientCaSecretRef(String clientCaSecretRef) { this.clientCaSecretRef = clientCaSecretRef; }
}
