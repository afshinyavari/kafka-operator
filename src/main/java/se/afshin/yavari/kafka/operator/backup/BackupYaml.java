package se.afshin.yavari.kafka.operator.backup;

/** Minimal YAML scalar helpers for hand-rendering kafka-backup config files. */
final class BackupYaml {

    private BackupYaml() {}

    /** Double-quotes and escapes a scalar so YAML never mis-parses values that start
     *  with {@code *}, contain {@code :}, etc. */
    static String q(String s) {
        if (s == null) {
            return "\"\"";
        }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
