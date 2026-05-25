package se.afshin.yavari.kafka.editor.model;

/** A catalog topic — its name, serialization formats, and value record type. */
public record TopicDef(
        String id,
        String name,
        Integer partitions,
        String keyFormat,
        String valueFormat,
        String valueRecordTypeId) {
}
