package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void disabledByDefaultReadsNothingElse() {
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(Map.of()), p);
        assertFalse(c.enabled());
        assertTrue(p.isEmpty());
        assertNull(c.schema());
    }

    @Test
    void enabledStringDefaults() {
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(Map.of("PRODUCER_ENABLED", "true", "PRODUCER_TOPIC", "orders")), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("orders", c.topic());
        assertEquals(1000L, c.intervalMs());
        assertEquals(Format.STRING, c.format());
        assertNull(c.schema());
    }

    @Test
    void enabledWithoutTopicIsAProblem() {
        Problems p = new Problems();
        ProducerConfig.from(env(Map.of("PRODUCER_ENABLED", "true")), p);
        assertEquals("PRODUCER_TOPIC is required", p.message());
    }

    @Test
    void avroReadsSchemaWithProducerPrefix() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_FORMAT", "avro");
        m.put("PRODUCER_INTERVAL_MS", "250");
        m.put("PRODUCER_SCHEMA_URL", "http://r/apis/registry/v3");
        Problems p = new Problems();
        ProducerConfig c = ProducerConfig.from(env(m), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(Format.AVRO, c.format());
        assertEquals(250L, c.intervalMs());
        assertNotNull(c.schema());
        assertEquals("http://r/apis/registry/v3", c.schema().url());
    }

    @Test
    void avroWithoutUrlIsAProblem() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_FORMAT", "avro");
        Problems p = new Problems();
        ProducerConfig.from(env(m), p);
        assertEquals("PRODUCER_SCHEMA_URL is required", p.message());
    }

    @Test
    void intervalMustBePositive() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "orders");
        m.put("PRODUCER_INTERVAL_MS", "0");
        Problems p = new Problems();
        ProducerConfig.from(env(m), p);
        assertTrue(p.message().contains("PRODUCER_INTERVAL_MS must be > 0"));
    }
}
