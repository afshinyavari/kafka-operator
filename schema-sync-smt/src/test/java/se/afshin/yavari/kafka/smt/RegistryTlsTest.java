package se.afshin.yavari.kafka.smt;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.KeyStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies {@link RegistryTls} against an in-process HTTPS server that requires a client
 *  certificate, for each supported keystore format. */
class RegistryTlsTest {

    @TempDir
    static Path dir;
    static TestCerts certs;
    static HttpsServer server;
    static String url;

    @BeforeAll
    static void startServer() throws Exception {
        certs = new TestCerts(dir);
        KeyStore ks = load(certs.serverP12);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, TestCerts.PASSWORD.toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(load(certs.serverTrustP12));
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ctx) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters p = ctx.getDefaultSSLParameters();
                p.setNeedClientAuth(true);
                params.setSSLParameters(p);
            }
        });
        server.createContext("/ping", ex -> {
            byte[] body = "pong".getBytes();
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
            ex.close();
        });
        server.start();
        url = "https://127.0.0.1:" + server.getAddress().getPort() + "/ping";
    }

    @AfterAll
    static void stopServer() {
        if (server != null) server.stop(0);
    }

    private static KeyStore load(Path p) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(p.toFile())) {
            ks.load(in, TestCerts.PASSWORD.toCharArray());
        }
        return ks;
    }

    private static String get(SSLContext ctx) throws Exception {
        HttpClient http = HttpClient.newBuilder().sslContext(ctx).build();
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return r.body();
    }

    @Test
    void pkcs12KeystoreAndTruststoreAuthenticate() throws Exception {
        SSLContext ctx = RegistryTls.build(new RegistryTls.Settings(
                certs.clientP12.toString(), TestCerts.PASSWORD, "PKCS12", null,
                certs.clientTrustP12.toString(), TestCerts.PASSWORD, "PKCS12"));
        assertThat(get(ctx)).isEqualTo("pong");
    }

    @Test
    void jksKeystoreAndTruststoreAuthenticate() throws Exception {
        SSLContext ctx = RegistryTls.build(new RegistryTls.Settings(
                certs.clientJks.toString(), TestCerts.PASSWORD, "JKS", null,
                certs.clientTrustJks.toString(), TestCerts.PASSWORD, "JKS"));
        assertThat(get(ctx)).isEqualTo("pong");
    }

    @Test
    void pemCertificateAndKeyAuthenticate() throws Exception {
        SSLContext ctx = RegistryTls.build(new RegistryTls.Settings(
                certs.clientCertPem.toString(), null, "PEM", certs.clientKeyPem.toString(),
                certs.serverCertPem.toString(), null, "PEM"));
        assertThat(get(ctx)).isEqualTo("pong");
    }

    @Test
    void truststoreOnlyIsRejectedWhenServerRequiresClientCert() throws Exception {
        SSLContext ctx = RegistryTls.build(new RegistryTls.Settings(
                null, null, "PKCS12", null,
                certs.clientTrustP12.toString(), TestCerts.PASSWORD, "PKCS12"));
        assertThatThrownBy(() -> get(ctx)).isInstanceOf(java.io.IOException.class);
    }

    @Test
    void noSettingsYieldsNullContext() throws Exception {
        assertThat(RegistryTls.build(new RegistryTls.Settings(null, null, "PKCS12", null, null, null, "PKCS12")))
                .isNull();
    }

    @Test
    void pemKeystoreWithoutKeyFileIsAConfigError() {
        assertThatThrownBy(() -> RegistryTls.build(new RegistryTls.Settings(
                certs.clientCertPem.toString(), null, "PEM", null, null, null, "PKCS12")))
                .isInstanceOf(org.apache.kafka.common.config.ConfigException.class)
                .hasMessageContaining("key.location");
    }
}
