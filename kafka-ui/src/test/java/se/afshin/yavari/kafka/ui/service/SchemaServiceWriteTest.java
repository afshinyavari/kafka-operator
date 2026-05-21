package se.afshin.yavari.kafka.ui.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaServiceWriteTest {

    @Test
    void validateContent_rejectsBlank() {
        assertThatThrownBy(() -> SchemaService.validateContent("AVRO", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");

        assertThatThrownBy(() -> SchemaService.validateContent("JSON", "   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");

        assertThatThrownBy(() -> SchemaService.validateContent("AVRO", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateContent_rejectsBadJsonForJsonAndAvro() {
        assertThatThrownBy(() -> SchemaService.validateContent("JSON", "{not json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid JSON");

        assertThatThrownBy(() -> SchemaService.validateContent("AVRO", "{not json"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validateContent_acceptsValidJson_forJsonAndAvro() {
        // Should not throw
        SchemaService.validateContent("JSON", "{\"type\":\"object\"}");
        SchemaService.validateContent("AVRO", "{\"type\":\"record\",\"name\":\"X\",\"fields\":[]}");
    }

    @Test
    void validateContent_protobuf_passesThroughWithoutJsonCheck() {
        // .proto syntax — would fail JSON parsing, but should be accepted as PROTOBUF.
        SchemaService.validateContent("PROTOBUF", "syntax = \"proto3\"; message X { string s = 1; }");
    }

    @Test
    void contentTypeFor_mapsKnownTypes() {
        assertThat(SchemaService.contentTypeFor("PROTOBUF")).isEqualTo("application/x-protobuf");
        assertThat(SchemaService.contentTypeFor("AVRO")).isEqualTo("application/json");
        assertThat(SchemaService.contentTypeFor("JSON")).isEqualTo("application/json");
        assertThat(SchemaService.contentTypeFor("JSONSCHEMA")).isEqualTo("application/json");
        // unknown -> default JSON
        assertThat(SchemaService.contentTypeFor("UNKNOWN")).isEqualTo("application/json");
        assertThat(SchemaService.contentTypeFor(null)).isEqualTo("application/json");
    }
}
