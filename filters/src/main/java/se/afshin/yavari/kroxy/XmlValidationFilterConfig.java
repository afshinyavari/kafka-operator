package se.afshin.yavari.kroxy;

import com.fasterxml.jackson.annotation.JsonProperty;

public class XmlValidationFilterConfig {

    @JsonProperty(required = true)
    private String bootstrapServers;

    @JsonProperty(required = true)
    private String schemaTopic;

    private int apiPort = 8080;

    private int validationThreadPoolSize = 4;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getSchemaTopic() { return schemaTopic; }
    public void setSchemaTopic(String schemaTopic) { this.schemaTopic = schemaTopic; }

    public int getApiPort() { return apiPort; }
    public void setApiPort(int apiPort) { this.apiPort = apiPort; }

    public int getValidationThreadPoolSize() { return validationThreadPoolSize; }
    public void setValidationThreadPoolSize(int validationThreadPoolSize) { this.validationThreadPoolSize = validationThreadPoolSize; }
}
