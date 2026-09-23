package se.afshin.yavari.kafka.smt;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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

    // ---- error classification: only not-found goes through behavior.on.error ----

    @Test
    void transientTargetFailureIsRetriableAndNotCached() {
        FakeConfluent.Schema s = confluentSource.register("orders-value", "AVRO", "o", List.of());
        apicurioTarget.failNextRequests.set(1);
        apicurioTarget.failStatus = 503;
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "behavior.on.error", "WARN"))) {
            byte[] in = confluent(s.id(), "p");
            // WARN must NOT pass the record through with the source id: retriable instead.
            assertThatThrownBy(() -> smt.apply(record("orders", in)))
                    .isInstanceOf(RetriableException.class)
                    .hasMessageContaining("transient")
                    .hasMessageContaining("503");
            // Nothing remembered: the next record succeeds once the target is back.
            SourceRecord out = smt.apply(record("orders", in));
            assertThat((byte[]) out.value()).isEqualTo(apicurio(1L, "p"));
        }
        assertThat(apicurioTarget.upsertCalls.get()).isEqualTo(1);
    }

    @Test
    void transientSourceFailureIsRetriableUnderIgnoreToo() {
        FakeConfluent.Schema s = confluentSource.register("orders-value", "AVRO", "o", List.of());
        confluentSource.failNextRequests.set(1);
        confluentSource.failStatus = 500;
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "behavior.on.error", "IGNORE"))) {
            byte[] in = confluent(s.id(), "p");
            assertThatThrownBy(() -> smt.apply(record("orders", in))).isInstanceOf(RetriableException.class);
            assertThat(smt.apply(record("orders", in)).value()).isEqualTo(apicurio(1L, "p"));
        }
        // The injected 500 short-circuits before the counter; the retry is the one real fetch.
        assertThat(confluentSource.fetchCalls.get()).isEqualTo(1);
    }

    @Test
    void unreachableRegistryIsRetriable() {
        // Nothing listens on the port: connection refused → transient.
        try (var smt = newSmt(Map.of("source.url", "http://127.0.0.1:1", "behavior.on.error", "WARN"))) {
            assertThatThrownBy(() -> smt.apply(record("orders", apicurio(1L, "p"))))
                    .isInstanceOf(RetriableException.class);
        }
    }

    @Test
    void permanentTargetRejectionIsAConnectExceptionEvenUnderWarn() {
        FakeConfluent.Schema s = confluentSource.register("orders-value", "AVRO", "o", List.of());
        apicurioTarget.failNextRequests.set(1);
        apicurioTarget.failStatus = 409; // incompatible schema
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "behavior.on.error", "WARN"))) {
            byte[] in = confluent(s.id(), "p");
            assertThatThrownBy(() -> smt.apply(record("orders", in)))
                    .isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class)
                    .hasMessageContaining("permanent")
                    .hasMessageContaining("409");
            // Not negative-cached either: a fixed target is used on the next record.
            assertThat(smt.apply(record("orders", in)).value()).isEqualTo(apicurio(1L, "p"));
        }
    }

    @Test
    void notFoundStillPassesThroughUnderWarnAndIsRemembered() {
        byte[] garbage = new byte[12];
        try (var smt = newSmt(Map.of("behavior.on.error", "WARN"))) {
            assertThat(smt.apply(record("raw", garbage)).value()).isSameAs(garbage);
            assertThat(smt.apply(record("raw", garbage)).value()).isSameAs(garbage);
        }
        assertThat(apicurioSource.metadataCalls.get()).isEqualTo(1);
    }

    @Test
    void missingReferencedSchemaIsPermanentNotPassthrough() {
        // The parent references "address"@1 whose globalId resolves but whose content is gone.
        apicurioSource.register(1L, "default", "address", "JSON", "{\"addr\":true}");
        apicurioSource.register(2L, "default", "order", "JSON", "{\"order\":true}",
                List.of(new SchemaRef("address.json", "default", "address", "1")));
        apicurioSource.byGlobalId.remove(1L); // meta still lists it; content 404s
        try (var smt = newSmt(Map.of("behavior.on.error", "WARN"))) {
            assertThatThrownBy(() -> smt.apply(record("orders", apicurio(2L, "p"))))
                    .isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class)
                    .hasMessageContaining("Referenced schema globalId=1");
        }
        assertThat(apicurioTarget.upsertCalls.get()).isZero();
    }

    // ---- subject choice ----

    @Test
    void sourceModePicksLexicographicallySmallestOwningSubject() {
        // Same content under two subjects → one Confluent id owned by both. Registered
        // "zebra" first so registration order and lexicographic order differ.
        FakeConfluent.Schema z = confluentSource.register("zebra-value", "AVRO", "\"string\"", List.of());
        FakeConfluent.Schema a = confluentSource.register("apple-value", "AVRO", "\"string\"", List.of());
        assertThat(a.id()).isEqualTo(z.id());
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT"))) {
            smt.apply(record("zebra", confluent(z.id(), "p")));
        }
        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("apple-value");
    }

    @Test
    void topicModeDerivesSubjectFromRecordTopicAndSide() {
        FakeConfluent.Schema s = confluentSource.register("whatever", "AVRO", "\"string\"", List.of());
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.mode", "topic",
                "apply.to", "BOTH"))) {
            SourceRecord out = smt.apply(new SourceRecord(null, null, "prod.orders", null, null,
                    confluent(s.id(), "k"), null, confluent(s.id(), "v")));
            assertThat((byte[]) out.key()).isEqualTo(apicurio(1L, "k"));
            assertThat((byte[]) out.value()).isEqualTo(apicurio(2L, "v"));
        }
        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("prod.orders-key", "prod.orders-value");
    }

    @Test
    void topicModeRegistersSharedSchemaUnderEveryTopicSubject() {
        FakeConfluent.Schema s = confluentSource.register("a-value", "AVRO", "\"string\"", List.of());
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.mode", "TOPIC"))) {
            smt.apply(record("a", confluent(s.id(), "p")));
            smt.apply(record("b", confluent(s.id(), "p")));
            smt.apply(record("a", confluent(s.id(), "p"))); // cache hit per (id, subject)
        }
        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("a-value", "b-value");
    }

    @Test
    void topicModeIgnoresPrefixAndKeepsReferenceSubjects() {
        FakeConfluent.Schema addr = confluentSource.register("address", "AVRO", "a", List.of());
        FakeConfluent.Schema order = confluentSource.register("orders-value", "AVRO", "o",
                List.of(new FakeConfluent.Ref("address", "address", addr.version())));
        try (var smt = newSmt(Map.of("source.format", "CONFLUENT", "target.subject.mode", "TOPIC",
                "target.subject.prefix", "dr."))) {
            smt.apply(record("dr.orders", confluent(order.id(), "p")));
            smt.apply(record("dr.address", confluent(addr.id(), "q")));
        }
        assertThat(apicurioTarget.upserts).extracting(FakeApicurio.Artifact::artifactId)
                .containsExactly("address", "dr.orders-value", "dr.address-value");
    }

    @Test
    void unknownSubjectModeIsAConfigError() {
        assertThatThrownBy(() -> newSmt(Map.of("target.subject.mode", "RECORD")))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("target.subject.mode");
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
            // Without the client keystore the registry rejects the handshake. That is a
            // transient failure: retriable, never a passthrough (even under WARN).
            cfg.remove("source.ssl.keystore.location");
            cfg.remove("source.ssl.keystore.password");
            try (var smt = newSmt(cfg)) {
                byte[] in = confluent(s.id(), "p");
                assertThatThrownBy(() -> smt.apply(record("t", in))).isInstanceOf(RetriableException.class);
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
