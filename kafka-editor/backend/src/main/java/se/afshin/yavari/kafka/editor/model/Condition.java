package se.afshin.yavari.kafka.editor.model;

/** A single field comparison within a predicate. */
public record Condition(String id, String field, String operator, String value) {
}
