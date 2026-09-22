package se.afshin.yavari.clientapp.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.avro.Event;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.ProducerConfig;

import java.time.Clock;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProducerRunnerTest {

    /** Serializer that accepts String or Event without touching a registry. */
    private static final Serializer<Object> ANY = (topic, data) -> String.valueOf(data).getBytes();

    private static ProducerConfig cfg(Format f) {
        return new ProducerConfig(true, "orders", 1000L, f, null);
    }

    @Test
    void stringFormatSendsJsonWithIdKey() {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        List<ProducerRecord<String, Object>> hist = mock.history();
        assertEquals(1, hist.size());
        ProducerRecord<String, Object> rec = hist.get(0);
        assertEquals("orders", rec.topic());
        assertInstanceOf(String.class, rec.value());
        assertTrue(((String) rec.value()).contains("\"sequence\":0"));
        assertTrue(((String) rec.value()).contains("\"id\":\"" + rec.key() + "\""));
        assertEquals(1, r.sent());
        assertEquals(0, r.failed());
    }

    @Test
    void avroFormatSendsEventObject() {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.AVRO), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        Object v = mock.history().get(0).value();
        assertInstanceOf(Event.class, v);
        assertEquals(mock.history().get(0).key(), ((Event) v).getId());
    }

    @Test
    void sendFailureIsCountedNotThrown() {
        MockProducer<String, Object> mock = new MockProducer<>(false, null, new StringSerializer(), ANY);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        r.tick();
        mock.errorNext(new RuntimeException("boom"));
        assertEquals(1, r.failed());
        assertEquals(0, r.sent());
    }

    @Test
    void synchronousSerializationErrorIsCountedNotThrown() {
        Serializer<Object> exploding = (topic, data) -> { throw new org.apache.kafka.common.errors.SerializationException("registry said no"); };
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), exploding);
        ProducerRunner r = new ProducerRunner(cfg(Format.STRING), mock, new PayloadGenerator("h", Clock.systemUTC()));
        assertDoesNotThrow(r::tick);
        assertEquals(1, r.failed());
    }

    @Test
    void startAndStopLifecycle() throws Exception {
        MockProducer<String, Object> mock = new MockProducer<>(true, null, new StringSerializer(), ANY);
        ProducerConfig fast = new ProducerConfig(true, "orders", 10L, Format.STRING, null);
        ProducerRunner r = new ProducerRunner(fast, mock, new PayloadGenerator("h", Clock.systemUTC()));
        assertFalse(r.isStarted());
        r.start();
        assertTrue(r.isStarted());
        Thread.sleep(100);
        r.stop();
        assertTrue(mock.closed());
        assertTrue(r.sent() >= 2, "expected several ticks, got " + r.sent());
    }
}
