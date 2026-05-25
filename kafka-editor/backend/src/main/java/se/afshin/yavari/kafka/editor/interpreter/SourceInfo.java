package se.afshin.yavari.kafka.editor.interpreter;

/** A source node in a built topology — what topic it reads and its value type. */
public record SourceInfo(String nodeId, String topicName, String recordTypeId) {
}
