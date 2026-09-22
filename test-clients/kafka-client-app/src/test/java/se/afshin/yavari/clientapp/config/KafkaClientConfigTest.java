package se.afshin.yavari.clientapp.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class KafkaClientConfigTest {

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
    void plaintextEmitsNoSslKeys() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9092");
        m.put("KAFKA_SECURITY_PROTOCOL", "plaintext");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-producer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("b:9092", props.get("bootstrap.servers"));
        assertEquals("PLAINTEXT", props.get("security.protocol"));
        assertEquals("kafka-client-app-producer", props.get("client.id"));
        assertTrue(props.keySet().stream().noneMatch(k -> k.toString().startsWith("ssl.")));
        assertTrue(props.keySet().stream().noneMatch(k -> k.toString().startsWith("sasl.")));
    }

    @Test
    void sslIsDefaultAndEmitsKeystoreAndTruststore() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9093");
        m.put("KAFKA_CLIENT_ID", "myapp");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-consumer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("SSL", props.get("security.protocol"));
        assertEquals("myapp-consumer", props.get("client.id"));
        assertEquals("/etc/tls/keystore.p12", props.get("ssl.keystore.location"));
        assertEquals("kp", props.get("ssl.keystore.password"));
        assertEquals("PKCS12", props.get("ssl.keystore.type"));
        assertEquals("/etc/tls/truststore.p12", props.get("ssl.truststore.location"));
        assertEquals("tp", props.get("ssl.truststore.password"));
        assertEquals("PKCS12", props.get("ssl.truststore.type"));
    }

    @Test
    void sslWithoutTruststoreIsAProblem() {
        Map<String, String> m = new HashMap<>();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9093");
        Problems p = new Problems();
        KafkaClientConfig.from(env(m), p);
        assertEquals(1, p.list().size());
        assertTrue(p.list().get(0).contains("KAFKA_TLS_TRUSTSTORE_PATH"));
    }

    @Test
    void saslSslEmitsOauthJaasAndTruststoreButNoKeystore() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9094");
        m.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        m.put("KAFKA_OAUTH_TOKEN_ENDPOINT", "https://kc/realms/r/protocol/openid-connect/token");
        m.put("KAFKA_OAUTH_CLIENT_ID", "cid");
        m.put("KAFKA_OAUTH_CLIENT_SECRET", "sec\"ret");
        m.put("KAFKA_OAUTH_SCOPE", "kafka");
        Problems p = new Problems();
        Properties props = KafkaClientConfig.from(env(m), p).toProperties("-producer");
        assertTrue(p.isEmpty(), p.message());
        assertEquals("SASL_SSL", props.get("security.protocol"));
        assertEquals("OAUTHBEARER", props.get("sasl.mechanism"));
        assertEquals("io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler",
                props.get("sasl.login.callback.handler.class"));
        String jaas = props.getProperty("sasl.jaas.config");
        assertTrue(jaas.startsWith("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "));
        assertTrue(jaas.contains("oauth.token.endpoint.uri=\"https://kc/realms/r/protocol/openid-connect/token\""));
        assertTrue(jaas.contains("oauth.client.id=\"cid\""));
        assertTrue(jaas.contains("oauth.client.secret=\"sec\\\"ret\""));
        assertTrue(jaas.contains("oauth.scope=\"kafka\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.location=\"/etc/tls/truststore.p12\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.password=\"tp\""));
        assertTrue(jaas.contains("oauth.ssl.truststore.type=\"PKCS12\""));
        assertTrue(jaas.endsWith(";"));
        assertEquals("/etc/tls/truststore.p12", props.get("ssl.truststore.location"));
        assertNull(props.get("ssl.keystore.location"));
    }

    @Test
    void saslSslMissingOauthVarsAreAllReported() {
        Map<String, String> m = tls();
        m.put("KAFKA_BOOTSTRAP_SERVERS", "b:9094");
        m.put("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        Problems p = new Problems();
        KafkaClientConfig.from(env(m), p);
        assertEquals(3, p.list().size());
        assertTrue(p.message().contains("KAFKA_OAUTH_TOKEN_ENDPOINT is required"));
        assertTrue(p.message().contains("KAFKA_OAUTH_CLIENT_ID is required"));
        assertTrue(p.message().contains("KAFKA_OAUTH_CLIENT_SECRET is required"));
    }

    @Test
    void missingBootstrapIsReported() {
        Problems p = new Problems();
        KafkaClientConfig.from(env(Map.of("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")), p);
        assertEquals("KAFKA_BOOTSTRAP_SERVERS is required", p.message());
    }
}
