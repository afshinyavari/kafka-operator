package se.afshin.yavari.kafka.editor.model;

/** One output field of a value mapping: `outputField` ← `expression`. */
public record ValueMappingEntry(
        String id,
        String outputField,
        ValueExpression expression) {
}
