package se.afshin.yavari.kafka.editor.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

/** Pure unit tests for the admin-client config builder — no broker needed. */
class AdminClientFactoryTest {

    @Test
    void normalizeDefaultsBlankToLocalhost() {
        assertEquals("localhost:9092", AdminClientFactory.normalize(null));
        assertEquals("localhost:9092", AdminClientFactory.normalize("   "));
    }

    @Test
    void normalizeTrimsExplicitServers() {
        assertEquals("broker-a:9092,broker-b:9092",
                AdminClientFactory.normalize("  broker-a:9092,broker-b:9092 "));
    }

    @Test
    void adminPropsCarriesBootstrapAndBoundedTimeouts() {
        Properties props = AdminClientFactory.adminProps("broker:9092");
        assertEquals("broker:9092",
                props.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(AdminClientFactory.TIMEOUT_MS,
                props.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals(AdminClientFactory.TIMEOUT_MS,
                props.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }
}
