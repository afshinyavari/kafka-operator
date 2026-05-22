package se.afshin.yavari.kafka.operator.crd;

/** Compression codec for backup segments. Maps to the osodevops backup config
 *  {@code backup.compression} field (rendered lower-case). */
public enum BackupCompression {
    NONE, ZSTD, LZ4
}
