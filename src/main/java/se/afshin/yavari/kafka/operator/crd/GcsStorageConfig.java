package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

/** Google Cloud Storage target. Credentials come from {@code credentialsSecretRef}. */
public class GcsStorageConfig {

    @Required
    private String bucket;

    private String prefix = "";

    /** Name of a Secret with key {@code key.json} — a GCP service-account key. */
    @Required
    private String credentialsSecretRef;

    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }

    public String getCredentialsSecretRef() { return credentialsSecretRef; }
    public void setCredentialsSecretRef(String credentialsSecretRef) {
        this.credentialsSecretRef = credentialsSecretRef;
    }
}
