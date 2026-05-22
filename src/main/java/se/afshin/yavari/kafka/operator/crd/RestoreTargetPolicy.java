package se.afshin.yavari.kafka.operator.crd;

/** Pre-flight guard applied to restore target topics before a KafkaRestore Job runs.
 *  Checked by the reconciler via AdminClient — see {@code KafkaRestoreReconciler}. */
public enum RestoreTargetPolicy {
    /** Every resolved target topic must not exist. */
    REQUIRE_ABSENT,
    /** Every resolved target topic must be absent or empty (no records). */
    REQUIRE_EMPTY,
    /** No guard — restore into topics that may already hold data. Dangerous. */
    ALLOW_NON_EMPTY
}
