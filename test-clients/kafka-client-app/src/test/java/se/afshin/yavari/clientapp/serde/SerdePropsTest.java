package se.afshin.yavari.clientapp.serde;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.Oidc;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;
import se.afshin.yavari.clientapp.config.TlsStores;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SerdePropsTest {

    private static final TlsStores BOTH = new TlsStores("/ks.p12", "kp", "PKCS12", "/ts.p12", "tp", "PKCS12");
    private static final TlsStores NONE = new TlsStores(null, null, "PKCS12", null, null, "PKCS12");

    private static SchemaRegistryConfig cfg(RegistryType t, String url, AuthMode a, TlsStores tls, Oidc oidc) {
        return new SchemaRegistryConfig(t, url, a, tls, oidc, true, "grp");
    }

    @Test
    void apicurioHttpNoneEmitsOnlyUrlRegisterGroupAndClass() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "http://r/apis/registry/v3", AuthMode.NONE, BOTH, null));
        assertEquals("http://r/apis/registry/v3", p.get("apicurio.registry.url"));
        assertEquals(true, p.get("apicurio.registry.auto-register"));
        assertEquals("grp", p.get("apicurio.registry.artifact.group-id"));
        assertEquals(SerdeProps.APICURIO_SERIALIZER, p.get("value.serializer"));
        assertTrue(p.keySet().stream().noneMatch(k -> k.contains(".tls.")));
        assertTrue(p.keySet().stream().noneMatch(k -> k.contains(".auth.")));
    }

    @Test
    void apicurioHttpsNoneEmitsTruststoreOnly() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r/apis/registry/v3", AuthMode.NONE, BOTH, null));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
        assertEquals("tp", p.get("apicurio.registry.tls.truststore.password"));
        assertEquals("PKCS12", p.get("apicurio.registry.tls.truststore.type"));
        assertNull(p.get("apicurio.registry.tls.keystore.location"));
    }

    @Test
    void apicurioMtlsEmitsKeystoreAndTruststore() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://proxy:8443/apis/registry/v3", AuthMode.MTLS, BOTH, null));
        assertEquals("/ks.p12", p.get("apicurio.registry.tls.keystore.location"));
        assertEquals("kp", p.get("apicurio.registry.tls.keystore.password"));
        assertEquals("PKCS12", p.get("apicurio.registry.tls.keystore.type"));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
    }

    @Test
    void apicurioOidcEmitsAuthKeysAndTruststore() {
        Oidc o = new Oidc("cid", "sec", "https://kc/token", "reg");
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r/apis/registry/v3", AuthMode.OIDC, BOTH, o));
        assertEquals("https://kc/token", p.get("apicurio.registry.auth.service.token.endpoint"));
        assertEquals("cid", p.get("apicurio.registry.auth.client.id"));
        assertEquals("sec", p.get("apicurio.registry.auth.client.secret"));
        assertEquals("reg", p.get("apicurio.registry.auth.client.scope"));
        assertEquals("/ts.p12", p.get("apicurio.registry.tls.truststore.location"));
        assertNull(p.get("apicurio.registry.tls.keystore.location"));
    }

    @Test
    void apicurioOidcWithoutScopeOmitsScopeKey() {
        Oidc o = new Oidc("cid", "sec", "https://kc/token", null);
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.APICURIO, "https://r", AuthMode.OIDC, NONE, o));
        assertFalse(p.containsKey("apicurio.registry.auth.client.scope"));
        assertFalse(p.containsKey("apicurio.registry.tls.truststore.location"));
    }

    @Test
    void apicurioConsumerSetsSpecificReaderAndDeserializer() {
        Map<String, Object> p = SerdeProps.consumer(cfg(RegistryType.APICURIO, "http://r", AuthMode.NONE, NONE, null));
        assertEquals(SerdeProps.APICURIO_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("apicurio.registry.use-specific-avro-reader"));
        assertFalse(p.containsKey("value.serializer"));
        assertFalse(p.containsKey("apicurio.registry.auto-register"));
    }

    @Test
    void confluentNoneHttp() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.CONFLUENT, "http://sr:8081", AuthMode.NONE, BOTH, null));
        assertEquals("http://sr:8081", p.get("schema.registry.url"));
        assertEquals(true, p.get("auto.register.schemas"));
        assertEquals(SerdeProps.CONFLUENT_SERIALIZER, p.get("value.serializer"));
        assertTrue(p.keySet().stream().noneMatch(k -> k.startsWith("schema.registry.ssl.")));
        assertTrue(p.keySet().stream().noneMatch(k -> k.startsWith("apicurio.")));
    }

    @Test
    void confluentMtlsHttps() {
        Map<String, Object> p = SerdeProps.producer(cfg(RegistryType.CONFLUENT, "https://sr:8081", AuthMode.MTLS, BOTH, null));
        assertEquals("/ts.p12", p.get("schema.registry.ssl.truststore.location"));
        assertEquals("tp", p.get("schema.registry.ssl.truststore.password"));
        assertEquals("PKCS12", p.get("schema.registry.ssl.truststore.type"));
        assertEquals("/ks.p12", p.get("schema.registry.ssl.keystore.location"));
        assertEquals("kp", p.get("schema.registry.ssl.keystore.password"));
        assertEquals("PKCS12", p.get("schema.registry.ssl.keystore.type"));
    }

    @Test
    void mtlsWithNullKeystoreFieldsEmitsNoKeystoreKeys() {
        TlsStores keystorePresentButFieldsNull = new TlsStores(null, null, "PKCS12", "/ts.p12", "tp", "PKCS12");
        Map<String, Object> p = SerdeProps.producer(
                cfg(RegistryType.APICURIO, "https://proxy:8443/apis/registry/v3", AuthMode.MTLS, keystorePresentButFieldsNull, null));
        assertTrue(p.keySet().stream().noneMatch(k -> k.contains(".keystore.")));
        assertTrue(p.values().stream().noneMatch(java.util.Objects::isNull));
    }

    @Test
    void confluentConsumer() {
        Map<String, Object> p = SerdeProps.consumer(cfg(RegistryType.CONFLUENT, "https://sr:8081", AuthMode.NONE, BOTH, null));
        assertEquals(SerdeProps.CONFLUENT_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("specific.avro.reader"));
        assertEquals("/ts.p12", p.get("schema.registry.ssl.truststore.location"));
        assertFalse(p.containsKey("auto.register.schemas"));
    }
}
