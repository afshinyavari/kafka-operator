package se.afshin.yavari.kroxy;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Config for the XML validation filter. {@code bootstrapServers} + {@code schemaTopic}
 * point at the Kafka cluster that holds the topic-compacted schema cache. When the
 * broker requires mTLS (e.g. KafkaCluster.spec.proxyMtls.enabled=true), set
 * {@code securityProtocol=SSL} and provide the three PEM cert paths so the internal
 * Kafka producer/consumer can authenticate. Paths typically point at the proxy's
 * mounted client cert (e.g. {@code /etc/proxy/kafka-tls/{tls.crt,tls.key,ca.crt}}).
 */
public class XmlValidationFilterConfig {

    @JsonProperty(required = true)
    private String bootstrapServers;

    @JsonProperty(required = true)
    private String schemaTopic;

    private int apiPort = 8080;

    private int validationThreadPoolSize = 4;

    /** "SSL" enables PEM-mode mTLS on the filter's internal Kafka client. Null/PLAINTEXT = plain. */
    private String securityProtocol;

    /** Path to the client certificate PEM (e.g. /etc/proxy/kafka-tls/tls.crt). */
    private String sslKeystoreCertPath;

    /** Path to the client private key PEM (PKCS#8, e.g. /etc/proxy/kafka-tls/tls.key). */
    private String sslKeystoreKeyPath;

    /** Path to the trust CA cert PEM (e.g. /etc/proxy/kafka-tls/ca.crt). */
    private String sslTruststoreCertPath;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getSchemaTopic() { return schemaTopic; }
    public void setSchemaTopic(String schemaTopic) { this.schemaTopic = schemaTopic; }

    public int getApiPort() { return apiPort; }
    public void setApiPort(int apiPort) { this.apiPort = apiPort; }

    public int getValidationThreadPoolSize() { return validationThreadPoolSize; }
    public void setValidationThreadPoolSize(int validationThreadPoolSize) { this.validationThreadPoolSize = validationThreadPoolSize; }

    public String getSecurityProtocol() { return securityProtocol; }
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

    public String getSslKeystoreCertPath() { return sslKeystoreCertPath; }
    public void setSslKeystoreCertPath(String sslKeystoreCertPath) { this.sslKeystoreCertPath = sslKeystoreCertPath; }

    public String getSslKeystoreKeyPath() { return sslKeystoreKeyPath; }
    public void setSslKeystoreKeyPath(String sslKeystoreKeyPath) { this.sslKeystoreKeyPath = sslKeystoreKeyPath; }

    public String getSslTruststoreCertPath() { return sslTruststoreCertPath; }
    public void setSslTruststoreCertPath(String sslTruststoreCertPath) { this.sslTruststoreCertPath = sslTruststoreCertPath; }
}
