package se.afshin.yavari.kafka.smt;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfluentClientTest {

    private HttpServer server;
    private FakeConfluent fake;
    private ConfluentClient client;

    @BeforeEach
    void start() throws IOException {
        fake = new FakeConfluent();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", fake);
        server.start();
        client = new ConfluentClient("http://127.0.0.1:" + server.getAddress().getPort() + "/",
                ApicurioClient.AuthProvider.none(), null);
    }

    @AfterEach
    void stop() {
        client.close();
        server.stop(0);
    }

    @Test
    void fetchByIdMapsSubjectToArtifactIdInDefaultGroup() throws Exception {
        FakeConfluent.Schema s = fake.register("orders-value", "AVRO", "{\"type\":\"string\"}", List.of());

        RegistrySchema schema = client.fetchById(s.id());

        assertThat(schema.groupId()).isEqualTo("default");
        assertThat(schema.artifactId()).isEqualTo("orders-value");
        assertThat(schema.type()).isEqualTo("AVRO");
        assertThat(new String(schema.content(), StandardCharsets.UTF_8)).isEqualTo("{\"type\":\"string\"}");
        assertThat(schema.references()).isEmpty();
    }

    @Test
    void fetchByIdOfUnknownIdIsNotFound() {
        assertThatThrownBy(() -> client.fetchById(4711))
                .isInstanceOf(RegistryException.class)
                .matches(e -> ((RegistryException) e).isNotFound());
    }

    @Test
    void fetchByIdPicksSmallestSubjectWhenIdIsShared() throws Exception {
        fake.register("zebra-value", "AVRO", "\"string\"", List.of());
        FakeConfluent.Schema a = fake.register("apple-value", "AVRO", "\"string\"", List.of());
        fake.register("mango-value", "AVRO", "\"string\"", List.of());

        assertThat(client.fetchById(a.id()).artifactId()).isEqualTo("apple-value");
    }

    @Test
    void fetchByIdCarriesSchemaTypeAndReferences() throws Exception {
        FakeConfluent.Schema addr = fake.register("address", "PROTOBUF", "message Address {}", List.of());
        FakeConfluent.Schema s = fake.register("orders-value", "PROTOBUF", "message Order {}",
                List.of(new FakeConfluent.Ref("address.proto", "address", addr.version())));

        RegistrySchema schema = client.fetchById(s.id());

        assertThat(schema.type()).isEqualTo("PROTOBUF");
        assertThat(schema.references()).containsExactly(
                new SchemaRef("address.proto", "default", "address", String.valueOf(addr.version())));
    }

    @Test
    void fetchByIdUnknownThrowsRegistryException() {
        assertThatThrownBy(() -> client.fetchById(999))
                .isInstanceOf(RegistryException.class)
                .hasMessageContaining("999");
    }

    @Test
    void upsertRegistersUnderSubjectAndReturnsIdAndVersion() throws Exception {
        fake.assignNextId(500);

        Registered r = client.upsert(new RegistrySchema("default", "orders-value", "AVRO",
                "{\"type\":\"string\"}".getBytes(StandardCharsets.UTF_8), List.of()));

        assertThat(r.id()).isEqualTo(500L);
        assertThat(r.version()).isEqualTo("1");
        assertThat(fake.bySubject.get("orders-value")).hasSize(1);
    }

    @Test
    void upsertIsIdempotentForIdenticalContent() throws Exception {
        RegistrySchema schema = new RegistrySchema("default", "orders-value", "AVRO",
                "{\"type\":\"string\"}".getBytes(StandardCharsets.UTF_8), List.of());

        Registered first = client.upsert(schema);
        Registered second = client.upsert(schema);

        assertThat(second).isEqualTo(first);
        assertThat(fake.bySubject.get("orders-value")).hasSize(1);
    }

    @Test
    void upsertSendsSchemaTypeAndReferences() throws Exception {
        FakeConfluent.Schema addr = fake.register("address", "PROTOBUF", "message Address {}", List.of());

        client.upsert(new RegistrySchema("default", "orders-value", "PROTOBUF",
                "message Order {}".getBytes(StandardCharsets.UTF_8),
                List.of(new SchemaRef("address.proto", "default", "address", String.valueOf(addr.version())))));

        FakeConfluent.Schema stored = fake.bySubject.get("orders-value").get(0);
        assertThat(stored.type()).isEqualTo("PROTOBUF");
        assertThat(stored.references()).containsExactly(new FakeConfluent.Ref("address.proto", "address", 1));
    }

    @Test
    void lookupIdResolvesSubjectAndVersion() throws Exception {
        fake.register("address", "AVRO", "v1", List.of());
        FakeConfluent.Schema v2 = fake.register("address", "AVRO", "v2", List.of());

        assertThat(client.lookupId(new SchemaRef("x", "default", "address", "2"))).isEqualTo((long) v2.id());
        assertThat(client.lookupId(new SchemaRef("x", "default", "address", null))).isEqualTo((long) v2.id());
        assertThat(client.lookupId(new SchemaRef("x", "default", "missing", "1"))).isNull();
    }

    @Test
    void retriesOnceWithRefreshedCredentialsAfter401() throws Exception {
        fake.requireAuthorization = "Bearer fresh";
        java.util.concurrent.atomic.AtomicInteger refreshes = new java.util.concurrent.atomic.AtomicInteger();
        ApicurioClient.AuthProvider auth = new ApicurioClient.AuthProvider() {
            public String header() { return refreshes.get() == 0 ? "Bearer stale" : "Bearer fresh"; }
            public boolean refresh() { refreshes.incrementAndGet(); return true; }
        };
        try (ConfluentClient c = new ConfluentClient(
                "http://127.0.0.1:" + server.getAddress().getPort(), auth, null)) {
            FakeConfluent.Schema s = fake.register("t", "AVRO", "x", List.of());
            assertThat(c.fetchById(s.id()).artifactId()).isEqualTo("t");
            assertThat(refreshes.get()).isEqualTo(1);
        }
    }

    @Test
    void upsertRejectsReferenceWithoutNumericVersion() {
        RegistrySchema noVersion = new RegistrySchema("default", "orders-value", "AVRO", "x".getBytes(),
                List.of(new SchemaRef("address", "default", "address", null)));
        assertThatThrownBy(() -> client.upsert(noVersion))
                .isInstanceOf(RegistryException.class)
                .hasMessageContaining("version");
        assertThat(fake.registerCalls.get()).isZero();
    }
}
