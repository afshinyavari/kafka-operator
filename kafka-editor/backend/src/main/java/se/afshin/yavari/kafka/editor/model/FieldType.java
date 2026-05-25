package se.afshin.yavari.kafka.editor.model;

import java.util.List;

/**
 * A field type — a discriminated union over `kind`
 * (primitive / enum / array / record / ref / unknown). Only the fields
 * relevant to a given kind are populated.
 */
public record FieldType(
        String kind,
        String primitive,
        List<String> symbols,
        FieldType items,
        List<RecordField> fields,
        String recordTypeId) {
}
