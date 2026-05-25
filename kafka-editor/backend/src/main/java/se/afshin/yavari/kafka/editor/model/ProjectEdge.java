package se.afshin.yavari.kafka.editor.model;

/** A directed connection between two node ports. */
public record ProjectEdge(
        String id,
        String source,
        String sourceHandle,
        String target,
        String targetHandle) {
}
