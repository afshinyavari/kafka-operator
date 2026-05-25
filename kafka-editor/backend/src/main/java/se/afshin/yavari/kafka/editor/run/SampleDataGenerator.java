package se.afshin.yavari.kafka.editor.run;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.model.FieldType;
import se.afshin.yavari.kafka.editor.model.RecordField;
import se.afshin.yavari.kafka.editor.model.RecordType;

/** Generates sample records from a record type, for test-mode runs. */
@ApplicationScoped
public class SampleDataGenerator {

    private static final int MAX_DEPTH = 4;

    @Inject
    ObjectMapper mapper;

    /** A sample record for the given record type; `index` varies the values. */
    public JsonNode generate(RecordType type, List<RecordType> all, int index) {
        if (type == null) {
            ObjectNode generic = mapper.createObjectNode();
            generic.put("id", "rec-" + index);
            generic.put("value", index);
            return generic;
        }
        return generateRecord(type, all, index, 0);
    }

    private JsonNode generateRecord(
            RecordType type, List<RecordType> all, int index, int depth) {
        ObjectNode object = mapper.createObjectNode();
        for (RecordField field : type.fields()) {
            if (field.name() == null || field.name().isEmpty()) {
                continue;
            }
            object.set(field.name(), generateField(field.type(), all, index, depth));
        }
        return object;
    }

    private JsonNode generateField(
            FieldType type, List<RecordType> all, int index, int depth) {
        if (type == null) {
            return NullNode.getInstance();
        }
        return switch (type.kind() == null ? "unknown" : type.kind()) {
            case "primitive" -> primitive(type.primitive(), index);
            case "enum" -> {
                List<String> symbols = type.symbols();
                yield symbols != null && !symbols.isEmpty()
                        ? TextNode.valueOf(symbols.get(index % symbols.size()))
                        : TextNode.valueOf("VALUE");
            }
            case "array" -> {
                ArrayNode array = mapper.createArrayNode();
                if (depth < MAX_DEPTH) {
                    array.add(generateField(type.items(), all, index, depth + 1));
                    array.add(generateField(type.items(), all, index + 1, depth + 1));
                }
                yield array;
            }
            case "record" -> {
                ObjectNode object = mapper.createObjectNode();
                if (type.fields() != null && depth < MAX_DEPTH) {
                    for (RecordField field : type.fields()) {
                        if (field.name() != null && !field.name().isEmpty()) {
                            object.set(field.name(),
                                    generateField(field.type(), all, index, depth + 1));
                        }
                    }
                }
                yield object;
            }
            case "ref" -> {
                RecordType ref = findById(all, type.recordTypeId());
                yield ref != null && depth < MAX_DEPTH
                        ? generateRecord(ref, all, index, depth + 1)
                        : NullNode.getInstance();
            }
            default -> NullNode.getInstance();
        };
    }

    private JsonNode primitive(String primitive, int index) {
        return switch (primitive == null ? "string" : primitive) {
            case "string" -> TextNode.valueOf("sample-" + index);
            case "boolean" -> BooleanNode.valueOf(index % 2 == 0);
            case "int" -> IntNode.valueOf(index);
            case "long" -> LongNode.valueOf(1000L + index);
            case "float", "double" ->
                    DoubleNode.valueOf(Math.round((10.0 + index * 1.5) * 100.0) / 100.0);
            case "bytes" -> TextNode.valueOf("bytes-" + index);
            case "date" ->
                    TextNode.valueOf("2026-01-" + String.format("%02d", 1 + index % 28));
            case "timestamp" -> LongNode.valueOf(1_700_000_000_000L + index * 1000L);
            case "uuid" -> TextNode.valueOf(UUID.randomUUID().toString());
            default -> TextNode.valueOf("value-" + index);
        };
    }

    private RecordType findById(List<RecordType> all, String id) {
        if (all == null || id == null) {
            return null;
        }
        for (RecordType type : all) {
            if (id.equals(type.id())) {
                return type;
            }
        }
        return null;
    }
}
