package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void disabledByDefault() {
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(Map.of()), p);
        assertFalse(c.enabled());
        assertTrue(p.isEmpty());
    }

    @Test
    void enabledStringDefaults() {
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(Map.of("CONSUMER_ENABLED", "true", "CONSUMER_TOPIC", "orders")), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("orders", c.topic());
        assertEquals("kafka-client-app", c.groupId());
        assertEquals("earliest", c.autoOffsetReset());
        assertEquals(Format.STRING, c.format());
        assertNull(c.schema());
    }

    @Test
    void avroReadsSchemaWithConsumerPrefix() {
        Map<String, String> m = new HashMap<>();
        m.put("CONSUMER_ENABLED", "true");
        m.put("CONSUMER_TOPIC", "orders");
        m.put("CONSUMER_GROUP_ID", "g1");
        m.put("CONSUMER_AUTO_OFFSET_RESET", "latest");
        m.put("CONSUMER_FORMAT", "AVRO");
        m.put("CONSUMER_SCHEMA_URL", "https://sr:8081");
        m.put("CONSUMER_SCHEMA_REGISTRY_TYPE", "confluent");
        Problems p = new Problems();
        ConsumerConfig c = ConsumerConfig.from(env(m), p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("g1", c.groupId());
        assertEquals("latest", c.autoOffsetReset());
        assertEquals(SchemaRegistryConfig.RegistryType.CONFLUENT, c.schema().type());
    }

    @Test
    void enabledWithoutTopicIsAProblem() {
        Problems p = new Problems();
        ConsumerConfig.from(env(Map.of("CONSUMER_ENABLED", "true")), p);
        assertEquals("CONSUMER_TOPIC is required", p.message());
    }
}
