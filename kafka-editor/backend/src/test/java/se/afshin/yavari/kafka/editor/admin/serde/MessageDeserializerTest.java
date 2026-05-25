package se.afshin.yavari.kafka.editor.admin.serde;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Pure unit tests for smart deserialization — no broker, no registry. */
class MessageDeserializerTest {

    private MessageDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new MessageDeserializer();
        deserializer.cache = new SchemaCache();
    }

    @Test
    void nullBytesAreATombstone() {
        Rendered r = deserializer.render(null, null);
        assertEquals("TOMBSTONE", r.strategy());
        assertNull(r.text());
    }

    @Test
    void zeroLengthBytesAreEmpty() {
        assertEquals("EMPTY", deserializer.render(new byte[0], null).strategy());
    }

    @Test
    void jsonObjectIsRecognisedAndPrettyPrinted() {
        Rendered r = deserializer.render("{\"name\":\"x\"}".getBytes(UTF_8), null);
        assertEquals("JSON", r.strategy());
        assertTrue(r.text().contains("name"));
    }

    @Test
    void plainTextIsRenderedAsString() {
        Rendered r = deserializer.render("hello world".getBytes(UTF_8), null);
        assertEquals("STRING", r.strategy());
        assertEquals("hello world", r.text());
    }

    @Test
    void binaryBytesFallBackToHex() {
        Rendered r = deserializer.render(
                new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD}, null);
        assertEquals("BINARY", r.strategy());
    }

    @Test
    void registryEnvelopeWithNoRegistryFallsThrough() {
        // 0x00 magic byte + 8-byte id + payload, but no registry to resolve it.
        byte[] bytes = {0x00, 0, 0, 0, 0, 0, 0, 0, 1, 9, 9, 9};
        Rendered r = deserializer.render(bytes, null);
        assertEquals("BINARY", r.strategy());
    }
}
