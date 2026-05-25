package se.afshin.yavari.kafka.editor.model;

/** A value — either a field reference (`path`) or a literal (`value`). */
public record ValueExpression(String kind, String path, String value) {
}
