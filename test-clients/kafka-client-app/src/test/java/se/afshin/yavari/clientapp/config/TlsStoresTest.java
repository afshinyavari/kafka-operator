package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TlsStoresTest {

    private static Env env(Map<String, String> m) { return new Env(m::get); }

    @Test
    void globalOnlyWithDefaultTypes() {
        TlsStores t = TlsStores.resolve(env(Map.of(
                "TLS_KEYSTORE_PATH", "/etc/tls/keystore.p12", "TLS_KEYSTORE_PASSWORD", "kp",
                "TLS_TRUSTSTORE_PATH", "/etc/tls/truststore.p12", "TLS_TRUSTSTORE_PASSWORD", "tp")),
                "KAFKA_TLS_");
        assertEquals("/etc/tls/keystore.p12", t.keystorePath());
        assertEquals("kp", t.keystorePassword());
        assertEquals("PKCS12", t.keystoreType());
        assertEquals("/etc/tls/truststore.p12", t.truststorePath());
        assertEquals("tp", t.truststorePassword());
        assertEquals("PKCS12", t.truststoreType());
        assertTrue(t.hasKeystore());
        assertTrue(t.hasTruststore());
    }

    @Test
    void componentOverridesSingleVariableOnly() {
        Map<String, String> m = new HashMap<>();
        m.put("TLS_KEYSTORE_PATH", "/g/ks.p12");
        m.put("TLS_KEYSTORE_PASSWORD", "gk");
        m.put("TLS_TRUSTSTORE_PATH", "/g/ts.p12");
        m.put("TLS_TRUSTSTORE_PASSWORD", "gt");
        m.put("PRODUCER_SCHEMA_TLS_TRUSTSTORE_PATH", "/p/ts.jks");
        m.put("PRODUCER_SCHEMA_TLS_TRUSTSTORE_TYPE", "jks");
        TlsStores t = TlsStores.resolve(env(m), "PRODUCER_SCHEMA_TLS_");
        assertEquals("/g/ks.p12", t.keystorePath());
        assertEquals("gk", t.keystorePassword());
        assertEquals("/p/ts.jks", t.truststorePath());
        assertEquals("gt", t.truststorePassword());
        assertEquals("JKS", t.truststoreType());
    }

    @Test
    void nothingSetMeansNoStores() {
        TlsStores t = TlsStores.resolve(env(Map.of()), "KAFKA_TLS_");
        assertFalse(t.hasKeystore());
        assertFalse(t.hasTruststore());
        assertNull(t.keystorePath());
        assertEquals("PKCS12", t.keystoreType());
    }
}
