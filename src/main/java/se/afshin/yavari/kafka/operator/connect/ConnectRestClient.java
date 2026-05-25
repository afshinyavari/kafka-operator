package se.afshin.yavari.kafka.operator.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * HTTP wrapper over the Kafka Connect REST API. Typed (not {@code JsonNode}-everything)
 * so the reconciler can diff and pattern-match cleanly.
 *
 * <p>MIRROR: kafka-editor/backend/src/main/java/.../admin/service/ConnectProxyService.java
 * — keep in sync until a shared module exists. The two diverge on return types
 * (passthrough vs typed), so duplication is the lesser evil for now.
 */
@ApplicationScoped
public class ConnectRestClient {

    private final ObjectMapper mapper = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    /** Returns the current config of a connector, or empty when Connect reports 404. */
    public Optional<Map<String, String>> getConfig(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/config")
                .GET().build());
        if (r.statusCode() == 404) return Optional.empty();
        ensure2xx(r, "GET /connectors/" + name + "/config");
        try {
            JsonNode node = mapper.readTree(r.body());
            Map<String, String> out = new LinkedHashMap<>();
            node.fields().forEachRemaining(e -> out.put(e.getKey(), e.getValue().asText()));
            return Optional.of(out);
        } catch (IOException e) {
            throw new ConnectRestException(r.statusCode(), "parse config: " + e.getMessage(), e);
        }
    }

    /** Creates a new connector via {@code POST /connectors}. */
    public void create(String baseUrl, String name, Map<String, String> config) {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("name", name);
            body.put("config", config);
            HttpResponse<String> r = send(req(baseUrl, "/connectors")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)))
                    .build());
            ensure2xx(r, "POST /connectors");
        } catch (IOException e) {
            throw new ConnectRestException(0, "serialize create: " + e.getMessage(), e);
        }
    }

    /** Updates a connector's config via {@code PUT /connectors/{name}/config}. Creates
     *  the connector if it does not exist (the REST API's documented behavior). */
    public void putConfig(String baseUrl, String name, Map<String, String> config) {
        try {
            HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/config")
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                    .build());
            ensure2xx(r, "PUT /connectors/" + name + "/config");
        } catch (IOException e) {
            throw new ConnectRestException(0, "serialize putConfig: " + e.getMessage(), e);
        }
    }

    public void pause(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/pause")
                .PUT(HttpRequest.BodyPublishers.noBody()).build());
        ensure2xx(r, "PUT /connectors/" + name + "/pause");
    }

    public void resume(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/resume")
                .PUT(HttpRequest.BodyPublishers.noBody()).build());
        ensure2xx(r, "PUT /connectors/" + name + "/resume");
    }

    public void stop(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/stop")
                .PUT(HttpRequest.BodyPublishers.noBody()).build());
        ensure2xx(r, "PUT /connectors/" + name + "/stop");
    }

    public void restart(String baseUrl, String name, boolean includeTasks, boolean onlyFailed) {
        String q = "?includeTasks=" + includeTasks + "&onlyFailed=" + onlyFailed;
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/restart" + q)
                .POST(HttpRequest.BodyPublishers.noBody()).build());
        ensure2xx(r, "POST /connectors/" + name + "/restart");
    }

    public void delete(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name)
                .DELETE().build());
        if (r.statusCode() == 404) return; // already gone — idempotent success
        ensure2xx(r, "DELETE /connectors/" + name);
    }

    /** Returns the {@code /connectors/{name}/status} response as a parsed JsonNode. The
     *  reconciler hands it to {@link ConnectStatusMapper}. Returns empty on 404. */
    public Optional<JsonNode> status(String baseUrl, String name) {
        HttpResponse<String> r = send(req(baseUrl, "/connectors/" + name + "/status")
                .GET().build());
        if (r.statusCode() == 404) return Optional.empty();
        ensure2xx(r, "GET /connectors/" + name + "/status");
        try {
            return Optional.of(mapper.readTree(r.body()));
        } catch (IOException e) {
            throw new ConnectRestException(r.statusCode(), "parse status: " + e.getMessage(), e);
        }
    }

    private HttpRequest.Builder req(String baseUrl, String path) {
        return HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/json");
    }

    private HttpResponse<String> send(HttpRequest req) {
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new ConnectRestException(0, "transport error: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectRestException(0, "interrupted", e);
        }
    }

    private static void ensure2xx(HttpResponse<String> r, String op) {
        int sc = r.statusCode();
        if (sc < 200 || sc >= 300) {
            throw new ConnectRestException(sc, op + ": HTTP " + sc + " — " + truncate(r.body()));
        }
    }

    private static String truncate(String body) {
        if (body == null) return "";
        return body.length() > 256 ? body.substring(0, 256) + "..." : body;
    }
}
