package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void neitherRunnerEnabledIsAnError() {
        ConfigException ex = assertThrows(ConfigException.class,
                () -> AppConfig.load(env(Map.of("KAFKA_BOOTSTRAP_SERVERS", "b:9092", "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))));
        assertEquals("at least one of PRODUCER_ENABLED or CONSUMER_ENABLED must be true", ex.getMessage());
    }

    @Test
    void allProblemsAreReportedTogether() {
        Map<String, String> m = new HashMap<>();
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_FORMAT", "avro");
        m.put("CONSUMER_ENABLED", "true");
        ConfigException ex = assertThrows(ConfigException.class, () -> AppConfig.load(env(m)));
        assertTrue(ex.problems().contains("KAFKA_BOOTSTRAP_SERVERS is required"));
        assertTrue(ex.problems().contains("PRODUCER_TOPIC is required"));
        assertTrue(ex.problems().contains("PRODUCER_SCHEMA_URL is required"));
        assertTrue(ex.problems().contains("CONSUMER_TOPIC is required"));
        assertTrue(ex.problems().stream().anyMatch(s -> s.contains("KAFKA_TLS_TRUSTSTORE_PATH")));
    }

    @Test
    void happyPathBothEnabled() {
        Map<String, String> m = new HashMap<>();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9092");
        m.put("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        m.put("PRODUCER_ENABLED", "true");
        m.put("PRODUCER_TOPIC", "t");
        m.put("CONSUMER_ENABLED", "true");
        m.put("CONSUMER_TOPIC", "t");
        AppConfig c = AppConfig.load(env(m));
        assertTrue(c.producer().enabled());
        assertTrue(c.consumer().enabled());
        assertEquals("b:9092", c.kafka().bootstrapServers());
    }
}
