package se.afshin.yavari.clientapp.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.avro.Event;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class PayloadGeneratorTest {

    private static final Clock FIXED = Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC);

    @Test
    void sequenceIncrementsAndFieldsAreFilled() {
        PayloadGenerator g = new PayloadGenerator("pod-1", FIXED);
        Event a = g.next();
        Event b = g.next();
        assertEquals(0L, a.getSequence());
        assertEquals(1L, b.getSequence());
        assertEquals(1_700_000_000_000L, a.getTimestamp());
        assertEquals("hello from pod-1 #0", a.getMessage());
        assertEquals("hello from pod-1 #1", b.getMessage());
        assertDoesNotThrow(() -> UUID.fromString(a.getId()));
        assertNotEquals(a.getId(), b.getId());
    }

    @Test
    void jsonHasExactlyTheFourFields() throws Exception {
        Event e = new PayloadGenerator("h", FIXED).next();
        JsonNode n = new ObjectMapper().readTree(PayloadGenerator.toJson(e));
        assertEquals(4, n.size());
        assertEquals(e.getId(), n.get("id").asText());
        assertEquals(0L, n.get("sequence").asLong());
        assertEquals(1_700_000_000_000L, n.get("timestamp").asLong());
        assertEquals("hello from h #0", n.get("message").asText());
    }

    @Test
    void avroRecordRoundTripsThroughItsOwnSchema() throws Exception {
        Event e = new PayloadGenerator("h", FIXED).next();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<>(Event.class).write(e, enc);
        enc.flush();
        BinaryDecoder dec = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        Event back = new SpecificDatumReader<>(Event.class).read(null, dec);
        assertEquals(e, back);
    }
}
