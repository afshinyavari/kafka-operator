package se.afshin.yavari.kafka.editor.model;

/** One field of a record type. */
public record RecordField(String name, FieldType type, Boolean nullable) {
}
