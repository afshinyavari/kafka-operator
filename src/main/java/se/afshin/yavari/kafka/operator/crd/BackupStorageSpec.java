package se.afshin.yavari.kafka.operator.crd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.generator.annotation.ValidationRule;

/** Discriminated union for the backup storage backend: exactly one of s3/azure/gcs/pvc. */
@ValidationRule(
        value = "[has(self.s3), has(self.azure), has(self.gcs), has(self.pvc)].filter(x, x).size() == 1",
        message = "exactly one of storage.s3, storage.azure, storage.gcs or storage.pvc must be set"
)
public class BackupStorageSpec {

    private S3StorageConfig s3;
    private AzureStorageConfig azure;
    private GcsStorageConfig gcs;
    private PvcStorageConfig pvc;

    public S3StorageConfig getS3() { return s3; }
    public void setS3(S3StorageConfig s3) { this.s3 = s3; }

    public AzureStorageConfig getAzure() { return azure; }
    public void setAzure(AzureStorageConfig azure) { this.azure = azure; }

    public GcsStorageConfig getGcs() { return gcs; }
    public void setGcs(GcsStorageConfig gcs) { this.gcs = gcs; }

    public PvcStorageConfig getPvc() { return pvc; }
    public void setPvc(PvcStorageConfig pvc) { this.pvc = pvc; }

    /** Resolves which backend is configured. Throws if zero or multiple are set
     *  (the CEL rule normally prevents that, but builders/tests call this directly). */
    @JsonIgnore
    public BackupStorageType resolveType() {
        BackupStorageType found = null;
        int count = 0;
        if (s3 != null)    { found = BackupStorageType.S3;    count++; }
        if (azure != null) { found = BackupStorageType.AZURE; count++; }
        if (gcs != null)   { found = BackupStorageType.GCS;   count++; }
        if (pvc != null)   { found = BackupStorageType.PVC;   count++; }
        if (count != 1) {
            throw new IllegalStateException(
                    "exactly one of storage.s3/azure/gcs/pvc must be set (found " + count + ")");
        }
        return found;
    }

    /** Name of the credentials Secret for cloud backends, or null for PVC storage. */
    @JsonIgnore
    public String credentialsSecretRef() {
        return switch (resolveType()) {
            case S3 -> s3.getCredentialsSecretRef();
            case AZURE -> azure.getCredentialsSecretRef();
            case GCS -> gcs.getCredentialsSecretRef();
            case PVC -> null;
        };
    }
}
