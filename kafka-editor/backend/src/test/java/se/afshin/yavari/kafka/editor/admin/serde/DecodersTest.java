package se.afshin.yavari.kafka.editor.admin.serde;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Pure unit tests for the Avro encode/decode round trip (schema-aware produce). */
class DecodersTest {

    private static final String SCHEMA = """
            {"type":"record","name":"User","fields":[
              {"name":"id","type":"int"},
              {"name":"name","type":"string"}]}""";

    @Test
    void avroEncodeThenDecodeRoundTrips() throws Exception {
        byte[] binary = Decoders.encodeAvro(SCHEMA, "{\"id\":7,\"name\":\"Ada\"}");
        String compact = Decoders.decodeAvro(SCHEMA, binary)
                .replaceAll("\\s", "");
        assertTrue(compact.contains("\"id\":7"), compact);
        assertTrue(compact.contains("\"name\":\"Ada\""), compact);
    }

    @Test
    void encodingRejectsAValueThatDoesNotMatchTheSchema() {
        assertThrows(Exception.class,
                () -> Decoders.encodeAvro(SCHEMA, "{\"id\":\"not-an-int\"}"));
    }
}
