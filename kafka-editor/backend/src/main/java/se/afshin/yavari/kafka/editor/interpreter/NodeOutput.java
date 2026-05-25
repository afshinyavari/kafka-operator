package se.afshin.yavari.kafka.editor.interpreter;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

/**
 * A node's interpreted output — exactly one of stream / table / grouped is
 * non-null, depending on what kind of value the node produces.
 */
public record NodeOutput(
        KStream<String, JsonNode> stream,
        KTable<String, JsonNode> table,
        KGroupedStream<String, JsonNode> grouped) {

    static NodeOutput ofStream(KStream<String, JsonNode> stream) {
        return new NodeOutput(stream, null, null);
    }

    static NodeOutput ofTable(KTable<String, JsonNode> table) {
        return new NodeOutput(null, table, null);
    }

    static NodeOutput ofGrouped(KGroupedStream<String, JsonNode> grouped) {
        return new NodeOutput(null, null, grouped);
    }

    /** Coerce to a KStream — a table becomes its changelog stream. */
    KStream<String, JsonNode> asStream() {
        if (stream != null) {
            return stream;
        }
        if (table != null) {
            return table.toStream();
        }
        return null;
    }

    /** Coerce to a KTable — a stream is materialized into one. */
    KTable<String, JsonNode> asTable() {
        if (table != null) {
            return table;
        }
        if (stream != null) {
            return stream.toTable();
        }
        return null;
    }
}
