package se.afshin.yavari.kafka.editor.model;

/** One accumulator field of an aggregate node — an op over an input field. */
public record AggregateField(
        String id,
        String name,
        String op,
        String sourceField) {
}
