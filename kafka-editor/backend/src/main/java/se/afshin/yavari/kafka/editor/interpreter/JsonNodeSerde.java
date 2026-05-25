package se.afshin.yavari.kafka.editor.interpreter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** Kafka Streams Serde for `JsonNode` — the generic record value type. */
public final class JsonNodeSerde implements Serde<JsonNode> {

    private final ObjectMapper mapper;

    public JsonNodeSerde(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /** No-arg constructor so this can be used as a Kafka Streams default serde. */
    public JsonNodeSerde() {
        this(new ObjectMapper());
    }

    @Override
    public Serializer<JsonNode> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize record", e);
            }
        };
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                return mapper.readTree(bytes);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize record", e);
            }
        };
    }
}
