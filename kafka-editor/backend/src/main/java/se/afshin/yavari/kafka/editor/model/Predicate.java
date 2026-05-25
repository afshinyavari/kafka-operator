package se.afshin.yavari.kafka.editor.model;

import java.util.List;

/** A boolean predicate — conditions joined by one combinator (AND / OR). */
public record Predicate(String combinator, List<Condition> conditions) {

    public List<Condition> conditions() {
        return conditions != null ? conditions : List.of();
    }
}
