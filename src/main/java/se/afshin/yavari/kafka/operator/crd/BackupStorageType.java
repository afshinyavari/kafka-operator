package se.afshin.yavari.kafka.operator.crd;

/** Storage backend discriminator, derived from which sub-config is set on
 *  {@link BackupStorageSpec}. Not a user-facing field. */
public enum BackupStorageType {
    S3, AZURE, GCS, PVC
}
