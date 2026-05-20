package se.afshin.yavari.kafka.ui.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(value = 3, unit = TimeUnit.SECONDS)
class MessageDeserializerTest {

    private static final String AVRO_USER = """
            {"type":"record","name":"User","fields":[
              {"name":"id","type":"int"},
              {"name":"name","type":"string"}
            ]}""";

    private SchemaCache cache;
    private MessageDeserializer deser;

    @BeforeEach
    void setUp() {
        cache = mock(SchemaCache.class);
        when(cache.byGlobalId(org.mockito.ArgumentMatchers.anyLong(), anyString(), anyString()))
                .thenReturn(Optional.empty());
        when(cache.byArtifact(anyString(), anyString(), anyString()))
                .thenReturn(Optional.empty());
        deser = new MessageDeserializer();
        deser.cache = cache;
    }

    @Test
    void nullPayloadIsTombstone() {
        Rendered r = deser.render("orders", MessageDeserializer.Side.VALUE, null, "http://x", "t");
        assertThat(r.strategy()).isEqualTo("TOMBSTONE");
    }

    @Test
    void emptyPayloadIsEmpty() {
        Rendered r = deser.render("orders", MessageDeserializer.Side.VALUE, new byte[0], "http://x", "t");
        assertThat(r.strategy()).isEqualTo("EMPTY");
    }

    @Test
    void apicurioAvroEnvelope_decodes() throws Exception {
        byte[] payload = encodeAvroUser(42, "alice");
        byte[] full = withApicurioEnvelope(7L, payload);
        when(cache.byGlobalId(eq(7L), anyString(), anyString()))
                .thenReturn(Optional.of(new SchemaCache.SchemaMeta("AVRO", AVRO_USER, 7L)));

        Rendered r = deser.render("users", MessageDeserializer.Side.VALUE, full, "http://x", "t");

        assertThat(r.strategy()).isEqualTo("APICURIO");
        assertThat(r.schemaRef()).isEqualTo("7");
        assertThat(r.text()).contains("\"alice\"").contains("42");
    }

    @Test
    void apicurioMiss_fallsThroughToHeuristic() {
        // Envelope-looking bytes but globalId not in registry → fall through.
        byte[] full = withApicurioEnvelope(999L, "{\"a\":1}".getBytes());
        // Hint: the apicurio miss is recorded; the rest of the bytes start with
        // 0x00... which is NOT '{' so JSON heuristic also misses.
        // Result: BINARY (hex) with a warning about the miss.
        Rendered r = deser.render("anything", MessageDeserializer.Side.VALUE, full, "http://x", "t");

        assertThat(r.strategy()).isEqualTo("BINARY");
        assertThat(r.warnings()).anyMatch(w -> w.contains("globalId=999"));
    }

    @Test
    void plainJsonIsRecognised() {
        byte[] body = "{\"hello\":\"world\"}".getBytes();
        Rendered r = deser.render("topic", MessageDeserializer.Side.VALUE, body, "http://x", "t");
        assertThat(r.strategy()).isEqualTo("JSON");
        assertThat(r.text()).contains("\"hello\" : \"world\"");
    }

    @Test
    void utf8StringIsRecognised() {
        byte[] body = "plain message text".getBytes();
        Rendered r = deser.render("topic", MessageDeserializer.Side.VALUE, body, "http://x", "t");
        assertThat(r.strategy()).isEqualTo("STRING");
        assertThat(r.text()).isEqualTo("plain message text");
    }

    @Test
    void binaryGarbageFallsBackToHex() {
        byte[] body = new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0x80, (byte) 0xC1};
        Rendered r = deser.render("topic", MessageDeserializer.Side.VALUE, body, "http://x", "t");
        assertThat(r.strategy()).isEqualTo("BINARY");
        assertThat(r.text()).contains("ff fe 80 c1");
    }

    @Test
    void topicConvention_decodesWithoutEnvelope() throws Exception {
        // Raw Avro bytes, no envelope. Lookup by name "orders-value" hits.
        byte[] payload = encodeAvroUser(1, "bob");
        when(cache.byArtifact(eq("orders-value"), anyString(), anyString()))
                .thenReturn(Optional.of(new SchemaCache.SchemaMeta("AVRO", AVRO_USER, 1L)));

        Rendered r = deser.render("orders", MessageDeserializer.Side.VALUE, payload, "http://x", "t");

        assertThat(r.strategy()).isEqualTo("TOPIC_CONV");
        assertThat(r.schemaRef()).isEqualTo("orders-value");
        assertThat(r.text()).contains("\"bob\"");
    }

    @Test
    void protobufType_rendersHexWithWarning() {
        byte[] payload = new byte[]{0x08, 0x2a};  // varint(1) = 42 — but we won't decode
        byte[] full = withApicurioEnvelope(12L, payload);
        when(cache.byGlobalId(eq(12L), anyString(), anyString()))
                .thenReturn(Optional.of(new SchemaCache.SchemaMeta("PROTOBUF",
                        "message Foo { int32 id = 1; }", 12L)));

        Rendered r = deser.render("t", MessageDeserializer.Side.VALUE, full, "http://x", "t");

        assertThat(r.strategy()).isEqualTo("APICURIO");
        assertThat(r.warnings()).anyMatch(w -> w.contains("Protobuf"));
        assertThat(r.text()).contains("08 2a");
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static byte[] encodeAvroUser(int id, String name) throws Exception {
        Schema schema = new Schema.Parser().parse(AVRO_USER);
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("id", id);
        rec.put("name", name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new org.apache.avro.generic.GenericDatumWriter<>(schema).write(rec, enc);
        enc.flush();
        return out.toByteArray();
    }

    private static byte[] withApicurioEnvelope(long globalId, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + payload.length);
        buf.put((byte) 0x00);
        buf.putLong(globalId);
        buf.put(payload);
        return buf.array();
    }
}
