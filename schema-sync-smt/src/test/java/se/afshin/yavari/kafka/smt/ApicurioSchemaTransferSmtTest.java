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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    private FakeApicurio source;
    private FakeApicurio target;

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
    }

    @AfterEach
    void stop() {
        if (sourceServer != null) sourceServer.stop(0);
        if (targetServer != null) targetServer.stop(0);
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

    /** In-process Apicurio stub. Implements just the endpoints the SMT calls. */
    static class FakeApicurio implements com.sun.net.httpserver.HttpHandler {
        record Artifact(String groupId, String artifactId, String type, String content) {}
        final Map<Long, Artifact> byGlobalId = new HashMap<>();
        long nextAssignedGlobalId = 1L;
        final AtomicInteger metadataCalls = new AtomicInteger();
        final AtomicInteger upsertCalls = new AtomicInteger();

        void register(long globalId, String groupId, String artifactId, String type, String content) {
            byGlobalId.put(globalId, new Artifact(groupId, artifactId, type, content));
        }

        void assignNextGlobalId(long id) { nextAssignedGlobalId = id; }

        @Override
        public void handle(HttpExchange ex) throws IOException {
            try {
                String path = ex.getRequestURI().getPath();
                String method = ex.getRequestMethod();
                // GET /apis/registry/v3/ids/globalIds/{id} (metadata + content double-duty
                // in the SMT — we serve the metadata JSON when Accept is JSON, raw content
                // otherwise — but the SMT uses two GETs to the same path; we always return JSON
                // for the first call and raw content for the second by checking metadataCalls).
                if (method.equals("GET") && path.matches("^/apis/registry/v3/ids/globalIds/\\d+$")) {
                    long id = Long.parseLong(path.substring(path.lastIndexOf('/') + 1));
                    Artifact a = byGlobalId.get(id);
                    if (a == null) {
                        ex.sendResponseHeaders(404, -1);
                        return;
                    }
                    int call = metadataCalls.incrementAndGet();
                    byte[] body;
                    if (call % 2 == 1) {
                        // Metadata
                        body = MAPPER.writeValueAsBytes(Map.of(
                                "groupId", a.groupId(),
                                "artifactId", a.artifactId(),
                                "artifactType", a.type(),
                                "globalId", id));
                        ex.getResponseHeaders().add("Content-Type", "application/json");
                    } else {
                        // Content
                        body = a.content().getBytes(StandardCharsets.UTF_8);
                    }
                    ex.sendResponseHeaders(200, body.length);
                    try (OutputStream os = ex.getResponseBody()) { os.write(body); }
                    return;
                }
                if (method.equals("GET") && path.matches("^/apis/registry/v3/ids/globalIds/\\d+/references$")) {
                    byte[] body = "[]".getBytes(StandardCharsets.UTF_8);
                    ex.getResponseHeaders().add("Content-Type", "application/json");
                    ex.sendResponseHeaders(200, body.length);
                    try (OutputStream os = ex.getResponseBody()) { os.write(body); }
                    return;
                }
                if (method.equals("POST") && path.startsWith("/apis/registry/v3/groups/")) {
                    upsertCalls.incrementAndGet();
                    JsonNode req = MAPPER.readTree(ex.getRequestBody());
                    long assigned = nextAssignedGlobalId++;
                    Map<String, Object> ver = Map.of("globalId", assigned);
                    byte[] body = MAPPER.writeValueAsBytes(Map.of("version", ver));
                    ex.getResponseHeaders().add("Content-Type", "application/json");
                    ex.sendResponseHeaders(200, body.length);
                    try (OutputStream os = ex.getResponseBody()) { os.write(body); }
                    return;
                }
                ex.sendResponseHeaders(404, -1);
            } finally {
                ex.close();
            }
        }
    }
}
