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

    @Test
    void keystorePathWithoutPasswordRecordsProblem() {
        Problems p = new Problems();
        TlsStores.resolve(env(Map.of("KAFKA_TLS_KEYSTORE_PATH", "/ks.p12")), "KAFKA_TLS_", p);
        assertEquals(1, p.list().size());
        assertEquals("KAFKA_TLS_KEYSTORE_PASSWORD (or TLS_KEYSTORE_PASSWORD) is required when a keystore path is set",
                p.list().get(0));
    }

    @Test
    void truststorePathWithoutPasswordRecordsProblem() {
        Problems p = new Problems();
        TlsStores.resolve(env(Map.of("KAFKA_TLS_TRUSTSTORE_PATH", "/ts.p12")), "KAFKA_TLS_", p);
        assertEquals(1, p.list().size());
        assertEquals("KAFKA_TLS_TRUSTSTORE_PASSWORD (or TLS_TRUSTSTORE_PASSWORD) is required when a truststore path is set",
                p.list().get(0));
    }

    @Test
    void badKeystoreTypeRecordsProblem() {
        Problems p = new Problems();
        TlsStores.resolve(env(Map.of("KAFKA_TLS_KEYSTORE_TYPE", "bks")), "KAFKA_TLS_", p);
        assertEquals(1, p.list().size());
        assertEquals("KAFKA_TLS_KEYSTORE_TYPE must be PKCS12 or JKS, got 'BKS'", p.list().get(0));
    }

    @Test
    void badTruststoreTypeRecordsProblem() {
        Problems p = new Problems();
        TlsStores.resolve(env(Map.of("KAFKA_TLS_TRUSTSTORE_TYPE", "bks")), "KAFKA_TLS_", p);
        assertEquals(1, p.list().size());
        assertEquals("KAFKA_TLS_TRUSTSTORE_TYPE must be PKCS12 or JKS, got 'BKS'", p.list().get(0));
    }

    @Test
    void pkcs12AndJksTypesAreAcceptedCaseInsensitively() {
        Problems p1 = new Problems();
        TlsStores t1 = TlsStores.resolve(env(Map.of("KAFKA_TLS_KEYSTORE_TYPE", "pkcs12")), "KAFKA_TLS_", p1);
        assertTrue(p1.isEmpty(), p1.message());
        assertEquals("PKCS12", t1.keystoreType());

        Problems p2 = new Problems();
        TlsStores t2 = TlsStores.resolve(env(Map.of("KAFKA_TLS_KEYSTORE_TYPE", "jks")), "KAFKA_TLS_", p2);
        assertTrue(p2.isEmpty(), p2.message());
        assertEquals("JKS", t2.keystoreType());
    }
}
