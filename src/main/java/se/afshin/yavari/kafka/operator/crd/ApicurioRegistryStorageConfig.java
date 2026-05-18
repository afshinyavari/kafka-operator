package se.afshin.yavari.kafka.operator.crd;

public class ApicurioRegistryStorageConfig {
    private String type = "mem";
    private String jdbcUrl;
    private String jdbcSecretRef;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

    public String getJdbcSecretRef() { return jdbcSecretRef; }
    public void setJdbcSecretRef(String jdbcSecretRef) { this.jdbcSecretRef = jdbcSecretRef; }
}
