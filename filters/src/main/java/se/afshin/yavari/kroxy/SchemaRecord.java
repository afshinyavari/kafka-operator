package se.afshin.yavari.kroxy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaRecord {
    private String topic;
    private String xsd;
    private String uploadedAt;
    private String description;

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getXsd() { return xsd; }
    public void setXsd(String xsd) { this.xsd = xsd; }
    public String getUploadedAt() { return uploadedAt; }
    public void setUploadedAt(String uploadedAt) { this.uploadedAt = uploadedAt; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
}
