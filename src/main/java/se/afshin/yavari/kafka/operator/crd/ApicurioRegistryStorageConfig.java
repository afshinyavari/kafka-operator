package se.afshin.yavari.kafka.operator.crd;

public class ApicurioRegistryStorageConfig {
    private String type = "mem";
    private String jdbcUrl;
    private String jdbcSecretRef;

    // kafkasql backend
    private String clusterRef;
    private String kafkaTopic = "kafkasql-journal";
    /** Override partition count of the kafkasql journal topic. Apicurio v2.6 documents
     *  single-partition for ordering; >1 is unsupported by Apicurio upstream. Defaults
     *  to 1 when unset. */
    private Integer kafkaTopicPartitions;
    private String tlsSecretRef;
    /** CN inside {@link #tlsSecretRef} — used to scope per-registry ACLs on the broker. */
    private String principal;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

    public String getJdbcSecretRef() { return jdbcSecretRef; }
    public void setJdbcSecretRef(String jdbcSecretRef) { this.jdbcSecretRef = jdbcSecretRef; }

    public String getClusterRef() { return clusterRef; }
    public void setClusterRef(String clusterRef) { this.clusterRef = clusterRef; }

    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

    public Integer getKafkaTopicPartitions() { return kafkaTopicPartitions; }
    public void setKafkaTopicPartitions(Integer kafkaTopicPartitions) {
        this.kafkaTopicPartitions = kafkaTopicPartitions;
    }

    public String getTlsSecretRef() { return tlsSecretRef; }
    public void setTlsSecretRef(String tlsSecretRef) { this.tlsSecretRef = tlsSecretRef; }

    public String getPrincipal() { return principal; }
    public void setPrincipal(String principal) { this.principal = principal; }
}
