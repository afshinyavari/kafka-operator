package se.afshin.yavari.kafka.editor.admin.dto;

/** Where a message-browse page starts reading. */
public enum SeekMode {
    /** From an explicit offset. */
    OFFSET,
    /** From the first record at or after a timestamp. */
    TIMESTAMP,
    /** The most recent records in the partition. */
    LATEST
}
