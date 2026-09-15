package se.afshin.yavari.kafka.smt;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SMT behaviour across registry formats: Confluent source → Apicurio target, the
 *  negative cache, and {@code ssl.*} wiring. Apicurio→Apicurio lives in
 *  {@link ApicurioSchemaTransferSmtTest}. */
class SchemaTransferFormatsTest {

    private HttpServer sourceServer;
    private HttpServer targetServer;
    private FakeConfluent confluentSource;
    private FakeApicurio apicurioSource;
    private FakeApicurio apicurioTarget;

    @BeforeEach
    void start() throws IOException {
        confluentSource = new FakeConfluent();
        apicurioSource = new FakeApicurio();
        apicurioTarget = new FakeApicurio();
        // One source server hosts both stubs: Apicurio under /apis, Confluent at the root.
        sourceServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        sourceServer.createContext("/apis", apicurioSource);
        sourceServer.createContext("/", confluentSource);
        sourceServer.start();
        targetServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        targetServer.createContext("/", apicurioTarget);
        targetServer.start();
    }

    @AfterEach
    void stop() {
        sourceServer.stop(0);
        targetServer.stop(0);
    }

    private ApicurioSchemaTransferSmt<SourceRecord> newSmt(Map<String, Object> overrides) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ApicurioSchemaTransferSmt.SOURCE_URL, "http://127.0.0.1:" + sourceServer.getAddress().getPort());
        cfg.put(ApicurioSchemaTransferSmt.TARGET_URL, "http://127.0.0.1:" + targetServer.getAddress().getPort());
        cfg.putAll(overrides);
        ApicurioSchemaTransferSmt<SourceRecord> smt = new ApicurioSchemaTransferSmt<>();
        smt.configure(cfg);
        return smt;
    }

    private static SourceRecord record(String topic, byte[] value) {
        return new SourceRecord(null, null, topic, null, null, null, null, value);
    }

    private static byte[] confluent(int id, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(5 + data.length).put((byte) 0).putInt(id).put(data).array();
    }

    private static byte[] apicurio(long id, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(9 + data.length).put((byte) 0).putLong(id).put(data).array();
    }

    @Test
    void confluentSourceToApicurioTargetReframesEnvelope() {
        FakeConfluent.Schema s = confluentSource.register("orders-value", "AVRO", "{\"type\":\"string\"}", List.of());
        apicurioTarget.assignNextGlobalId(9001L);
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.format", "APICURIO"))) {
            SourceRecord out = smt.apply(record("orders", confluent(s.id(), "abc")));

            assertThat((byte[]) out.value()).isEqualTo(apicurio(9001L, "abc"));
        }
        FakeApicurio.Artifact stored = apicurioTarget.byGlobalId.get(9001L);
        assertThat(stored.groupId()).isEqualTo("default");
        assertThat(stored.artifactId()).isEqualTo("orders-value");
        assertThat(stored.type()).isEqualTo("AVRO");
        assertThat(stored.content()).isEqualTo("{\"type\":\"string\"}");
    }

    @Test
    void confluentSchemaTypeBecomesApicurioArtifactType() {
        FakeConfluent.Schema s = confluentSource.register("ev-value", "PROTOBUF", "message Ev {}", List.of());
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT"))) {
            smt.apply(record("ev", confluent(s.id(), "x")));
        }
        assertThat(apicurioTarget.upserts).singleElement()
                .extracting(FakeApicurio.Artifact::type).isEqualTo("PROTOBUF");
    }

    @Test
    void confluentReferencesAreRegisteredFirstWithTargetVersions() {
        // Target already has an unrelated version 1 of "address", so the mirrored one becomes version 2.
        apicurioTarget.register(100L, "default", "address", "PROTOBUF", "message Old {}");
        FakeConfluent.Schema addr = confluentSource.register("address", "PROTOBUF", "message Address {}", List.of());
        FakeConfluent.Schema order = confluentSource.register("orders-value", "PROTOBUF", "message Order {}",
                List.of(new FakeConfluent.Ref("address.proto", "address", addr.version())));
        apicurioTarget.assignNextGlobalId(200L);

        try (var smt = newSmt(Map.of("source.format", "CONFLUENT"))) {
            SourceRecord out = smt.apply(record("orders", confluent(order.id(), "p")));
            assertThat((byte[]) out.value()).isEqualTo(apicurio(201L, "p"));
        }

        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("address", "orders-value");
        FakeApicurio.Artifact parent = apicurioTarget.upserts.get(1);
        assertThat(parent.references()).containsExactly(
                new SchemaRef("address.proto", "default", "address", "2"));
    }

    @Test
    void apicurioReferencesAreRewrittenToTargetVersions() {
        apicurioTarget.register(100L, "default", "address", "JSON", "{\"old\":true}");
        apicurioSource.register(1L, "default", "address", "JSON", "{\"addr\":true}");
        apicurioSource.register(2L, "default", "order", "JSON", "{\"order\":true}",
                List.of(new SchemaRef("address.json", "default", "address", "1")));
        apicurioTarget.assignNextGlobalId(300L);

        try (var smt = newSmt(Map.of())) {
            SourceRecord out = smt.apply(record("orders", apicurio(2L, "p")));
            assertThat((byte[]) out.value()).isEqualTo(apicurio(301L, "p"));
        }

        assertThat(apicurioTarget.upserts.get(1).references()).containsExactly(
                new SchemaRef("address.json", "default", "address", "2"));
    }

    @Test
    void referencedSchemaIsCachedForDirectHits() {
        FakeConfluent.Schema addr = confluentSource.register("address", "AVRO", "a", List.of());
        FakeConfluent.Schema order = confluentSource.register("orders-value", "AVRO", "o",
                List.of(new FakeConfluent.Ref("address", "address", addr.version())));
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT"))) {
            smt.apply(record("orders", confluent(order.id(), "p")));
            int fetches = confluentSource.fetchCalls.get();
            smt.apply(record("address", confluent(addr.id(), "q")));
            assertThat(confluentSource.fetchCalls.get()).isEqualTo(fetches);
        }
    }

    @Test
    void negativeCacheStopsRepeatedLookupsForUnknownIds() {
        byte[] garbage = new byte[12]; // 0x00 prefix, looks like an envelope, no such schema
        try (var smt = newSmt(Map.of("behavior.on.error", "WARN"))) {
            for (int i = 0; i < 5; i++) {
                assertThat(smt.apply(record("raw", garbage)).value()).isSameAs(garbage);
            }
        }
        assertThat(apicurioSource.metadataCalls.get()).isEqualTo(1);
    }

    @Test
    void negativeCacheDisabledWithZeroTtl() {
        byte[] garbage = new byte[12];
        try (var smt = newSmt(Map.of("behavior.on.error", "WARN", "cache.negative.ttl.ms", 0))) {
            smt.apply(record("raw", garbage));
            smt.apply(record("raw", garbage));
        }
        assertThat(apicurioSource.metadataCalls.get()).isEqualTo(2);
    }

    @Test
    void negativeCacheStillFailsFastInFailMode() {
        byte[] garbage = new byte[12];
        try (var smt = newSmt(Map.of("behavior.on.error", "FAIL"))) {
            assertThatThrownBy(() -> smt.apply(record("raw", garbage))).isInstanceOf(ConnectException.class);
            assertThatThrownBy(() -> smt.apply(record("raw", garbage))).isInstanceOf(ConnectException.class);
        }
        assertThat(apicurioSource.metadataCalls.get()).isEqualTo(1);
    }

    @Test
    void targetSubjectPrefixIsAppliedToTopLevelSubjectOnly() {
        FakeConfluent.Schema addr = confluentSource.register("address", "AVRO", "a", List.of());
        FakeConfluent.Schema order = confluentSource.register("orders-value", "AVRO", "o",
                List.of(new FakeConfluent.Ref("address", "address", addr.version())));

        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.prefix", "prod."))) {
            smt.apply(record("prod.orders", confluent(order.id(), "p")));
        }

        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("address", "prod.orders-value");
        assertThat(apicurioTarget.upserts.get(1).references()).containsExactly(
                new SchemaRef("address", "default", "address", "1"));
    }

    @Test
    void targetSubjectPrefixAppliesToConfluentTargetsToo() {
        apicurioSource.register(1L, "default", "orders-value", "AVRO", "{\"type\":\"string\"}");
        FakeConfluent confluentTarget = new FakeConfluent();
        targetServer.removeContext("/");
        targetServer.createContext("/", confluentTarget);

        try (var smt = newSmt(Map.of("target.format", "CONFLUENT", "target.subject.prefix", "dr."))) {
            smt.apply(record("dr.orders", apicurio(1L, "p")));
        }

        assertThat(confluentTarget.bySubject).containsOnlyKeys("dr.orders-value");
    }

    @Test
    void withPrefixAReferencedSchemaSeenTopLevelIsRegisteredUnderPrefixedSubject() {
        FakeConfluent.Schema addr = confluentSource.register("address", "AVRO", "a", List.of());
        FakeConfluent.Schema order = confluentSource.register("orders-value", "AVRO", "o",
                List.of(new FakeConfluent.Ref("address", "address", addr.version())));
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.prefix", "prod."))) {
            smt.apply(record("prod.orders", confluent(order.id(), "p")));
            smt.apply(record("prod.address", confluent(addr.id(), "q")));
        }
        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("address", "prod.orders-value", "prod.address");
    }

    @Test
    void emptyTargetSubjectPrefixKeepsSourceSubject() {
        FakeConfluent.Schema s = confluentSource.register("orders-value", "AVRO", "o", List.of());
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.prefix", ""))) {
            smt.apply(record("orders", confluent(s.id(), "p")));
        }
        assertThat(apicurioTarget.upserts).singleElement()
                .extracting(FakeApicurio.Artifact::artifactId).isEqualTo("orders-value");
    }

    @Test
    void unknownFormatIsAConfigError() {
        assertThatThrownBy(() -> newSmt(Map.of("source.format", "PULSAR")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("source.format");
    }

    @Test
    void sslSettingsPresentClientCertificateToSourceRegistry(@TempDir Path dir) throws Exception {
        TestCerts certs = new TestCerts(dir);
        HttpsServer tls = httpsServer(certs);
        tls.createContext("/", confluentSource);
        tls.start();
        try {
            FakeConfluent.Schema s = confluentSource.register("t-value", "AVRO", "x", List.of());
            Map<String, Object> cfg = new HashMap<>();
            cfg.put("source.format", "CONFLUENT");
            cfg.put(ApicurioSchemaTransferSmt.SOURCE_URL, "https://127.0.0.1:" + tls.getAddress().getPort());
            cfg.put("source.ssl.keystore.location", certs.clientP12.toString());
            cfg.put("source.ssl.keystore.password", TestCerts.PASSWORD);
            cfg.put("source.ssl.truststore.location", certs.clientTrustP12.toString());
            cfg.put("source.ssl.truststore.password", TestCerts.PASSWORD);
            try (var smt = newSmt(cfg)) {
                SourceRecord out = smt.apply(record("t", confluent(s.id(), "p")));
                assertThat((byte[]) out.value()).isEqualTo(apicurio(1L, "p"));
            }
            // Without the client keystore the registry rejects the handshake and WARN passes through.
            cfg.remove("source.ssl.keystore.location");
            cfg.remove("source.ssl.keystore.password");
            try (var smt = newSmt(cfg)) {
                byte[] in = confluent(s.id(), "p");
                assertThat(smt.apply(record("t", in)).value()).isSameAs(in);
            }
        } finally {
            tls.stop(0);
        }
    }

    private static HttpsServer httpsServer(TestCerts certs) throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(certs.serverP12.toFile())) {
            ks.load(in, TestCerts.PASSWORD.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, TestCerts.PASSWORD.toCharArray());
        KeyStore ts = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(certs.serverTrustP12.toFile())) {
            ts.load(in, TestCerts.PASSWORD.toCharArray());
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ctx) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters p = ctx.getDefaultSSLParameters();
                p.setNeedClientAuth(true);
                params.setSSLParameters(p);
            }
        });
        return server;
    }
}
