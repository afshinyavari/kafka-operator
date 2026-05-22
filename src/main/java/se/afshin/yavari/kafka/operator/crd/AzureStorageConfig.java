package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

/** Azure Blob Storage target. Credentials come from {@code credentialsSecretRef}. */
public class AzureStorageConfig {

    @Required
    private String container;

    private String prefix = "";

    /** Name of a Secret with keys {@code accountName} and {@code accountKey}. */
    @Required
    private String credentialsSecretRef;

    public String getContainer() { return container; }
    public void setContainer(String container) { this.container = container; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }

    public String getCredentialsSecretRef() { return credentialsSecretRef; }
    public void setCredentialsSecretRef(String credentialsSecretRef) {
        this.credentialsSecretRef = credentialsSecretRef;
    }
}
