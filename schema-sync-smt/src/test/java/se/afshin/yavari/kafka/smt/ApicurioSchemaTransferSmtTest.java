package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the SMT against a tiny in-process HTTP server that mimics enough of Apicurio v3
 *  to exercise the envelope-rewrite + reference walk + error handling paths. */
class ApicurioSchemaTransferSmtTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer sourceServer;
    private HttpServer targetServer;
    private HttpServer tokenServer;
    private FakeApicurio source;
    private FakeApicurio target;
    private final AtomicInteger tokenCalls = new AtomicInteger();

    @BeforeEach
    void start() throws IOException {
        source = new FakeApicurio();
        target = new FakeApicurio();
        sourceServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        sourceServer.createContext("/", source);
        sourceServer.start();
        targetServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        targetServer.createContext("/", target);
        targetServer.start();
        // OAuth2 client-credentials token endpoint — issues a fresh token per request.
        tokenCalls.set(0);
        tokenServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        tokenServer.createContext("/token", ex -> {
            int n = tokenCalls.incrementAndGet();
            byte[] body = ("{\"access_token\":\"tok-" + n + "\",\"expires_in\":300}")
                    .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
            ex.close();
        });
        tokenServer.start();
    }

    @AfterEach
    void stop() {
        if (sourceServer != null) sourceServer.stop(0);
        if (targetServer != null) targetServer.stop(0);
        if (tokenServer != null) tokenServer.stop(0);
    }

    /** Writes the three OAuth credential files the SMT expects in an {@code auth.oauth.dir}. */
    private String writeOauthDir(Path dir) throws IOException {
        Files.writeString(dir.resolve("token-url"),
                "http://127.0.0.1:" + tokenServer.getAddress().getPort() + "/token");
        Files.writeString(dir.resolve("client-id"), "mm2-schema-sync");
        Files.writeString(dir.resolve("client-secret"), "secret");
        return dir.toString();
    }

    private ApicurioSchemaTransferSmt<SourceRecord> newSmt() {
        return newSmt(Map.of());
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

    private SourceRecord recordValue(String topic, byte[] value) {
        return new SourceRecord(null, null, topic, null, null, null, null, value);
    }

    private SourceRecord recordKv(String topic, byte[] key, byte[] value) {
        return new SourceRecord(null, null, topic, null, null, key, null, value);
    }

    private static byte[] envelope(long globalId, String payload) {
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + data.length);
        bb.put((byte) 0x00);
        bb.putLong(globalId);
        bb.put(data);
        return bb.array();
    }

    @Test
    void plainStringPassthrough() {
        try (var smt = newSmt()) {
            SourceRecord r = recordValue("t1", "hello world".getBytes(StandardCharsets.UTF_8));
            SourceRecord out = smt.apply(r);
            assertThat(out.value()).isEqualTo(r.value());
        }
    }

    @Test
    void jsonPassthrough() {
        try (var smt = newSmt()) {
            SourceRecord r = recordValue("t1", "{\"k\":1}".getBytes(StandardCharsets.UTF_8));
            assertThat(smt.apply(r).value()).isEqualTo(r.value());
        }
    }

    @Test
    void xmlPassthrough() {
        try (var smt = newSmt()) {
            SourceRecord r = recordValue("t1", "<root/>".getBytes(StandardCharsets.UTF_8));
            assertThat(smt.apply(r).value()).isEqualTo(r.value());
        }
    }

    @Test
    void utf16beXmlFalsePositiveSurvivesViaWarn() {
        // UTF-16BE encoded "<root/>" starts with 0x00 — false-positive envelope. Source registry
        // returns 404. With behavior.on.error=WARN (default), passthrough.
        try (var smt = newSmt()) {
            byte[] utf16xml = "<root/>".getBytes(StandardCharsets.UTF_16BE);
            SourceRecord r = recordValue("t1", utf16xml);
            SourceRecord out = smt.apply(r);
            assertThat(out.value()).isEqualTo(utf16xml);
        }
    }

    @Test
    void tombstonePassthrough() {
        try (var smt = newSmt()) {
            SourceRecord r = recordValue("t1", null);
            assertThat(smt.apply(r).value()).isNull();
        }
    }

    @Test
    void shortEnvelopeLengthPassthrough() {
        try (var smt = newSmt()) {
            byte[] tooShort = new byte[]{0x00, 0x01, 0x02};
            SourceRecord r = recordValue("t1", tooShort);
            assertThat(smt.apply(r).value()).isEqualTo(tooShort);
        }
    }

    @Test
    void envelopeRewriteOnCacheMiss() {
        source.register(42L, "default", "events-value", "AVRO", "{\"type\":\"string\"}");
        target.assignNextGlobalId(99L);

        try (var smt = newSmt()) {
            byte[] in = envelope(42L, "payload-bytes");
            SourceRecord r = recordValue("events", in);
            SourceRecord out = smt.apply(r);
            byte[] outBytes = (byte[]) out.value();
            assertThat(outBytes[0]).isEqualTo((byte) 0x00);
            long rewritten = ByteBuffer.wrap(outBytes, 1, 8).getLong();
            assertThat(rewritten).isEqualTo(99L);
            // payload preserved
            byte[] payload = new byte[outBytes.length - 9];
            System.arraycopy(outBytes, 9, payload, 0, payload.length);
            assertThat(new String(payload)).isEqualTo("payload-bytes");
            // target was called once
            assertThat(target.upsertCalls.get()).isEqualTo(1);
        }
    }

    @Test
    void cacheHitIssuesZeroExtraTargetCalls() {
        source.register(42L, "default", "events-value", "AVRO", "{\"type\":\"string\"}");
        target.assignNextGlobalId(99L);

        try (var smt = newSmt()) {
            smt.apply(recordValue("events", envelope(42L, "a")));
            int afterFirst = target.upsertCalls.get();
            smt.apply(recordValue("events", envelope(42L, "b")));
            smt.apply(recordValue("events", envelope(42L, "c")));
            assertThat(target.upsertCalls.get()).isEqualTo(afterFirst);
        }
    }

    @Test
    void behaviorOnErrorFailThrows() {
        // No artifact registered on source → 404 → FAIL re-throws as ConnectException.
        try (var smt = newSmt(Map.of(ApicurioSchemaTransferSmt.BEHAVIOR_ON_ERROR, "FAIL"))) {
            SourceRecord r = recordValue("events", envelope(7L, "x"));
            assertThatThrownBy(() -> smt.apply(r)).isInstanceOf(ConnectException.class);
        }
    }

    @Test
    void applyToValueDoesNotProcessKey() {
        // Key bytes look envelope-shaped but applyTo=VALUE (default) — key unchanged.
        try (var smt = newSmt()) {
            byte[] keyBytes = envelope(42L, "key-payload");
            SourceRecord r = recordKv("events", keyBytes, "plain".getBytes(StandardCharsets.UTF_8));
            SourceRecord out = smt.apply(r);
            assertThat(out.key()).isEqualTo(keyBytes); // unchanged
            assertThat(source.metadataCalls.get()).isZero(); // no lookup at all
        }
    }

    @Test
    void oauthAuthenticatedUpsertSucceeds(@TempDir Path tmp) throws IOException {
        // Target registry demands a bearer token; the SMT acquires one via client-credentials.
        source.register(42L, "default", "events-value", "AVRO", "{\"type\":\"string\"}");
        target.assignNextGlobalId(99L);
        target.requireBearer = "tok-1";

        try (var smt = newSmt(Map.of(
                ApicurioSchemaTransferSmt.TARGET_AUTH_OAUTH_DIR, writeOauthDir(tmp)))) {
            SourceRecord out = smt.apply(recordValue("events", envelope(42L, "p")));
            byte[] outBytes = (byte[]) out.value();
            assertThat(ByteBuffer.wrap(outBytes, 1, 8).getLong()).isEqualTo(99L);
            assertThat(target.upsertCalls.get()).isEqualTo(1);
            assertThat(tokenCalls.get()).isEqualTo(1);
        }
    }

    @Test
    void oauthRefreshesAndRetriesAfter401(@TempDir Path tmp) throws IOException {
        // First POST is rejected 401 (stale token) — the client must refresh + retry once.
        source.register(42L, "default", "events-value", "AVRO", "{\"type\":\"string\"}");
        target.assignNextGlobalId(99L);
        target.rejectPostsRemaining.set(1);

        try (var smt = newSmt(Map.of(
                ApicurioSchemaTransferSmt.TARGET_AUTH_OAUTH_DIR, writeOauthDir(tmp)))) {
            SourceRecord out = smt.apply(recordValue("events", envelope(42L, "p")));
            byte[] outBytes = (byte[]) out.value();
            assertThat(ByteBuffer.wrap(outBytes, 1, 8).getLong()).isEqualTo(99L);
            assertThat(target.upsertCalls.get()).isEqualTo(1);
            // token fetched once up front, then re-fetched after the 401
            assertThat(tokenCalls.get()).isEqualTo(2);
        }
    }

    /** In-process Apicurio <strong>v2</strong> stub. Implements just the endpoints the SMT calls. */
    static class FakeApicurio implements com.sun.net.httpserver.HttpHandler {
        record Artifact(String groupId, String artifactId, String type, String content) {}
        final Map<Long, Artifact> byGlobalId = new HashMap<>();
        long nextAssignedGlobalId = 1L;
        /** Counts content lookups (GET /ids/globalIds/{id}). */
        final AtomicInteger metadataCalls = new AtomicInteger();
        final AtomicInteger upsertCalls = new AtomicInteger();
        /** When set, any request lacking {@code Authorization: Bearer <requireBearer>} gets 401. */
        volatile String requireBearer;
        /** Number of upcoming POSTs to reject with 401 (simulates a stale token). */
        final AtomicInteger rejectPostsRemaining = new AtomicInteger();

        void register(long globalId, String groupId, String artifactId, String type, String content) {
            byGlobalId.put(globalId, new Artifact(groupId, artifactId, type, content));
        }

        void assignNextGlobalId(long id) { nextAssignedGlobalId = id; }

        @Override
        public void handle(HttpExchange ex) throws IOException {
            try {
                String path = ex.getRequestURI().getPath();
                String query = ex.getRequestURI().getQuery();
                String method = ex.getRequestMethod();
                if (requireBearer != null
                        && !("Bearer " + requireBearer).equals(
                                ex.getRequestHeaders().getFirst("Authorization"))) {
                    ex.sendResponseHeaders(401, -1);
                    return;
                }
                // GET /apis/registry/v2/ids/globalIds/{id} — raw schema content.
                if (method.equals("GET") && path.matches("^/apis/registry/v2/ids/globalIds/\\d+$")) {
                    metadataCalls.incrementAndGet();
                    Artifact a = byGlobalId.get(idFromPath(path));
                    if (a == null) { ex.sendResponseHeaders(404, -1); return; }
                    sendBytes(ex, 200, a.content().getBytes(StandardCharsets.UTF_8));
                    return;
                }
                // GET /apis/registry/v2/ids/globalIds/{id}/references — references list.
                if (method.equals("GET")
                        && path.matches("^/apis/registry/v2/ids/globalIds/\\d+/references$")) {
                    sendJson(ex, 200, "[]");
                    return;
                }
                // GET /apis/registry/v2/search/artifacts?globalId={id} — artifact identity.
                if (method.equals("GET") && path.equals("/apis/registry/v2/search/artifacts")) {
                    long id = Long.parseLong((query == null ? "" : query)
                            .replaceAll(".*globalId=(\\d+).*", "$1"));
                    Artifact a = byGlobalId.get(id);
                    sendJson(ex, 200, a == null
                            ? "{\"artifacts\":[],\"count\":0}"
                            : MAPPER.writeValueAsString(Map.of(
                                    "artifacts", java.util.List.of(
                                            Map.of("id", a.artifactId(), "type", a.type())),
                                    "count", 1)));
                    return;
                }
                // POST /apis/registry/v2/groups/{g}/artifacts — create; returns ArtifactMetaData.
                if (method.equals("POST")
                        && path.matches("^/apis/registry/v2/groups/[^/]+/artifacts$")) {
                    if (rejectPostsRemaining.getAndUpdate(x -> x > 0 ? x - 1 : 0) > 0) {
                        ex.sendResponseHeaders(401, -1);
                        return;
                    }
                    upsertCalls.incrementAndGet();
                    ex.getRequestBody().readAllBytes();
                    long assigned = nextAssignedGlobalId++;
                    sendJson(ex, 200, MAPPER.writeValueAsString(Map.of("globalId", assigned)));
                    return;
                }
                ex.sendResponseHeaders(404, -1);
            } finally {
                ex.close();
            }
        }

        private static long idFromPath(String path) {
            String[] parts = path.split("/");
            return Long.parseLong(parts[parts.length - 1]);
        }

        private static void sendJson(HttpExchange ex, int code, String body) throws IOException {
            ex.getResponseHeaders().add("Content-Type", "application/json");
            sendBytes(ex, code, body.getBytes(StandardCharsets.UTF_8));
        }

        private static void sendBytes(HttpExchange ex, int code, byte[] body) throws IOException {
            ex.sendResponseHeaders(code, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
        }
    }
}
