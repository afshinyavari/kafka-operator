package se.afshin.yavari.kroxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * Embedded HTTP API for managing XML schemas at runtime.
 *
 * POST   /schemas         — register or update a schema (JSON body: SchemaRecord)
 * DELETE /schemas/{topic} — remove a schema
 * GET    /schemas         — list all registered topics
 */
public class SchemaManagementApi implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SchemaManagementApi.class);

    private final XmlSchemaStore store;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpServer server;

    public SchemaManagementApi(XmlSchemaStore store) {
        this.store = store;
    }

    public void start(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/schemas", this::handle);
        server.setExecutor(Executors.newSingleThreadExecutor(r -> new Thread(r, "schema-api")));
        server.start();
        log.info("Schema management API listening on port {}", port);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            switch (exchange.getRequestMethod()) {
                case "GET" -> handleGet(exchange);
                case "POST" -> handlePost(exchange);
                case "DELETE" -> handleDelete(exchange);
                default -> respond(exchange, 405, "Method Not Allowed");
            }
        } catch (Exception e) {
            log.error("API request failed", e);
            respond(exchange, 500, "Internal error: " + e.getMessage());
        }
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        respond(exchange, 200, objectMapper.writeValueAsString(store.listTopics()));
    }

    private void handlePost(HttpExchange exchange) throws Exception {
        byte[] body = exchange.getRequestBody().readAllBytes();
        SchemaRecord record = objectMapper.readValue(body, SchemaRecord.class);
        if (record.getTopic() == null || record.getXsd() == null) {
            respond(exchange, 400, "Fields 'topic' and 'xsd' are required");
            return;
        }
        store.publishSchema(record);
        log.info("Schema registered for topic '{}' via API", record.getTopic());
        respond(exchange, 200, "Schema registered for topic: " + record.getTopic());
    }

    private void handleDelete(HttpExchange exchange) throws Exception {
        String path = exchange.getRequestURI().getPath();
        String[] parts = path.split("/");
        if (parts.length < 3 || parts[parts.length - 1].isBlank()) {
            respond(exchange, 400, "Missing topic name — use DELETE /schemas/{topic}");
            return;
        }
        String topic = parts[parts.length - 1];
        store.deleteSchema(topic);
        log.info("Schema deleted for topic '{}' via API", topic);
        respond(exchange, 200, "Schema deleted for topic: " + topic);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
    }
}
