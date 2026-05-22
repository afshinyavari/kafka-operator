package se.afshin.yavari.kafka.operator.backup;

/** Fixed in-container paths shared by the config builders and the workload builder. */
final class BackupPaths {

    private BackupPaths() {}

    /** Rendered kafka-backup YAML config (ConfigMap mount). */
    static final String CONFIG_DIR = "/config";
    /** Broker mTLS PEM material (Secret mount). */
    static final String TLS_DIR = "/etc/kafka-backup/tls";
    /** PVC storage root. */
    static final String PVC_DIR = "/backup";
    /** GCS service-account key (Secret mount). */
    static final String GCS_DIR = "/etc/kafka-backup/gcs";
    static final String GCS_KEY_FILE = GCS_DIR + "/key.json";
    /** Sub-directory (under the storage prefix/root) for the Apicurio schema export. */
    static final String SCHEMA_SUBDIR = "schemas";
}
