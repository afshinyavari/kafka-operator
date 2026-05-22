package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.Required;

/** S3 (or S3-compatible, e.g. MinIO) object-storage target. Credentials come from
 *  {@code credentialsSecretRef} — never inlined into the rendered config. */
public class S3StorageConfig {

    @Required
    private String bucket;

    /** Key prefix within the bucket. */
    private String prefix = "";

    private String region = "us-east-1";

    /** Custom endpoint for S3-compatible stores (MinIO). Empty = real AWS S3. */
    private String endpoint = "";

    /** Path-style addressing — required by MinIO and most S3-compatible stores. */
    private boolean pathStyleAccess = false;

    /** Name of a Secret with keys {@code accessKeyId} and {@code secretAccessKey}. */
    @Required
    private String credentialsSecretRef;

    public String getBucket() { return bucket; }
    public void setBucket(String bucket) { this.bucket = bucket; }

    public String getPrefix() { return prefix; }
    public void setPrefix(String prefix) { this.prefix = prefix; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public boolean isPathStyleAccess() { return pathStyleAccess; }
    public void setPathStyleAccess(boolean pathStyleAccess) { this.pathStyleAccess = pathStyleAccess; }

    public String getCredentialsSecretRef() { return credentialsSecretRef; }
    public void setCredentialsSecretRef(String credentialsSecretRef) {
        this.credentialsSecretRef = credentialsSecretRef;
    }
}
