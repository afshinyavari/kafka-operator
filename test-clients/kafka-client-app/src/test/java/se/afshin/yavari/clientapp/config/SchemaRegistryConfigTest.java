package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import static se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;

class SchemaRegistryConfigTest {

    private static final String P = "PRODUCER_SCHEMA_";

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    private static Map<String, String> tls() {
        Map<String, String> m = new HashMap<>();
        m.put("TLS_KEYSTORE_PATH", "/etc/tls/keystore.p12");
        m.put("TLS_KEYSTORE_PASSWORD", "kp");
        m.put("TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "tp");
        return m;
    }

    @Test
    void defaultsAreApicurioNoneAutoRegisterDefaultGroup() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "http://registry:8080/apis/registry/v3");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), P, p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(RegistryType.APICURIO, c.type());
        assertEquals(AuthMode.NONE, c.auth());
        assertTrue(c.autoRegister());
        assertEquals("default", c.group());
        assertFalse(c.https());
        assertNull(c.oidc());
    }

    @Test
    void urlIsRequired() {
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(Map.of()), P, p);
        assertEquals("PRODUCER_SCHEMA_URL is required", p.message());
    }

    @Test
    void mtlsRequiresKeystore() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://proxy:8443/apis/registry/v3");
        m.put(P + "AUTH", "mtls");
        m.put("TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "tp");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("PRODUCER_SCHEMA_TLS_KEYSTORE_PATH"));
    }

    @Test
    void mtlsUsesComponentPrefixForTls() {
        Map<String, String> m = tls();
        m.put("CONSUMER_SCHEMA_URL", "https://proxy:8443/apis/registry/v3");
        m.put("CONSUMER_SCHEMA_AUTH", "MTLS");
        m.put("CONSUMER_SCHEMA_TLS_KEYSTORE_PATH", "/other.p12");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), "CONSUMER_SCHEMA_", p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals("/other.p12", c.tls().keystorePath());
        assertTrue(c.https());
    }

    @Test
    void oidcRequiresThreeVarsAndReportsAll() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://registry/apis/registry/v3");
        m.put(P + "AUTH", "oidc");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(3, p.list().size());
        assertTrue(p.message().contains("PRODUCER_SCHEMA_CLIENT_ID is required"));
        assertTrue(p.message().contains("PRODUCER_SCHEMA_CLIENT_SECRET is required"));
        assertTrue(p.message().contains("PRODUCER_SCHEMA_TOKEN_ENDPOINT is required"));
    }

    @Test
    void oidcHappyPath() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://registry/apis/registry/v3");
        m.put(P + "AUTH", "oidc");
        m.put(P + "CLIENT_ID", "cid");
        m.put(P + "CLIENT_SECRET", "sec");
        m.put(P + "TOKEN_ENDPOINT", "https://kc/token");
        m.put(P + "SCOPE", "registry");
        m.put(P + "AUTO_REGISTER", "false");
        m.put(P + "GROUP", "orders");
        Problems p = new Problems();
        SchemaRegistryConfig c = SchemaRegistryConfig.from(env(m), P, p);
        assertTrue(p.isEmpty(), p.message());
        assertEquals(new SchemaRegistryConfig.Oidc("cid", "sec", "https://kc/token", "registry"), c.oidc());
        assertFalse(c.autoRegister());
        assertEquals("orders", c.group());
    }

    @Test
    void oidcRejectedForConfluent() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "https://sr:8081");
        m.put(P + "REGISTRY_TYPE", "confluent");
        m.put(P + "AUTH", "oidc");
        m.put(P + "CLIENT_ID", "cid");
        m.put(P + "CLIENT_SECRET", "sec");
        m.put(P + "TOKEN_ENDPOINT", "https://kc/token");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("PRODUCER_SCHEMA_AUTH=oidc"));
        assertTrue(p.list().get(0).contains("confluent"));
    }

    @Test
    void invalidEnumValuesReported() {
        Map<String, String> m = new HashMap<>();
        m.put(P + "URL", "http://x");
        m.put(P + "REGISTRY_TYPE", "glue");
        m.put(P + "AUTH", "basic");
        Problems p = new Problems();
        SchemaRegistryConfig.from(env(m), P, p);
        assertEquals(2, p.list().size());
    }
}
