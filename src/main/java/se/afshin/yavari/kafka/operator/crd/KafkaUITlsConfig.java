package se.afshin.yavari.kafka.operator.crd;

public class KafkaUITlsConfig {
    private String secretName = "kafka-proxy-test-client-tls";
    private String mountPath = "/etc/kafka-tls";

    public String getSecretName() { return secretName; }
    public void setSecretName(String secretName) { this.secretName = secretName; }

    public String getMountPath() { return mountPath; }
    public void setMountPath(String mountPath) { this.mountPath = mountPath; }
}
