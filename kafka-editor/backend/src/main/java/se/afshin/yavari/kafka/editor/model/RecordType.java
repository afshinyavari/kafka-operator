package se.afshin.yavari.kafka.editor.model;

import java.util.List;

/** A named record-type definition — the shape of a Kafka record value. */
public record RecordType(String id, String name, List<RecordField> fields) {

    public List<RecordField> fields() {
        return fields != null ? fields : List.of();
    }
}
