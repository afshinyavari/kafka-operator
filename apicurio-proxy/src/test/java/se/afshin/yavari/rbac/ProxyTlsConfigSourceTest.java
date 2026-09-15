package se.afshin.yavari.rbac;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyTlsConfigSourceTest {

    @Test
    void disabledByDefaultYieldsNoProperties() {
        assertThat(ProxyTlsConfigSource.properties(k -> null)).isEmpty();
    }

    @Test
    void pkcs12ServerAndTrustStores() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true",
                "PROXY_TLS_KEYSTORE", "/tls/server.p12", "PROXY_TLS_KEYSTORE_PASSWORD", "spw",
                "PROXY_TLS_TRUSTSTORE", "/tls/clients-ca.p12", "PROXY_TLS_TRUSTSTORE_PASSWORD", "tpw");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.p12.path", "/tls/server.p12")
                .containsEntry("quarkus.tls.key-store.p12.password", "spw")
                .containsEntry("quarkus.tls.trust-store.p12.path", "/tls/clients-ca.p12")
                .containsEntry("quarkus.tls.trust-store.p12.password", "tpw")
                .containsEntry("quarkus.http.insecure-requests", "disabled")
                .containsEntry("quarkus.http.ssl-port", "8443");
    }

    @Test
    void jksAndCustomPort() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true", "PROXY_TLS_PORT", "9443",
                "PROXY_TLS_KEYSTORE", "/tls/server.jks", "PROXY_TLS_KEYSTORE_PASSWORD", "spw",
                "PROXY_TLS_KEYSTORE_TYPE", "JKS",
                "PROXY_TLS_TRUSTSTORE", "/tls/ca.jks", "PROXY_TLS_TRUSTSTORE_PASSWORD", "tpw",
                "PROXY_TLS_TRUSTSTORE_TYPE", "JKS");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.jks.path", "/tls/server.jks")
                .containsEntry("quarkus.tls.trust-store.jks.path", "/tls/ca.jks")
                .containsEntry("quarkus.http.ssl-port", "9443")
                .doesNotContainKey("quarkus.tls.key-store.p12.path");
    }

    @Test
    void pemFiles() {
        Map<String, String> env = Map.of(
                "PROXY_TLS_ENABLED", "true", "PROXY_TLS_KEYSTORE_TYPE", "PEM",
                "PROXY_TLS_CERT", "/tls/tls.crt", "PROXY_TLS_KEY", "/tls/tls.key",
                "PROXY_TLS_TRUSTSTORE_TYPE", "PEM", "PROXY_TLS_CA", "/tls/ca.crt");
        Map<String, String> p = ProxyTlsConfigSource.properties(env::get);
        assertThat(p).containsEntry("quarkus.tls.key-store.pem.server.cert", "/tls/tls.crt")
                .containsEntry("quarkus.tls.key-store.pem.server.key", "/tls/tls.key")
                .containsEntry("quarkus.tls.trust-store.pem.certs", "/tls/ca.crt");
    }

    @Test
    void enabledWithoutKeystoreIsAnError() {
        Map<String, String> env = Map.of("PROXY_TLS_ENABLED", "true");
        assertThatThrownBy(() -> ProxyTlsConfigSource.properties(env::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROXY_TLS_KEYSTORE");
    }
}
