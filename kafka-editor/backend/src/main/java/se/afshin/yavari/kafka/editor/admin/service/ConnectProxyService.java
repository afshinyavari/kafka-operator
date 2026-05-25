package se.afshin.yavari.kafka.editor.admin.service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;

/**
 * Proxies the Kafka Connect REST API (Connect is not an AdminClient feature).
 * The Connect base URL is supplied per request — same pattern as the registry
 * proxy.
 */
@ApplicationScoped
public class ConnectProxyService {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Inject
    ObjectMapper mapper;

    public JsonNode connectors(String connectUrl) {
        return request(connectUrl, "GET",
                "/connectors?expand=status&expand=info", null);
    }

    public JsonNode status(String connectUrl, String name) {
        return request(connectUrl, "GET",
                "/connectors/" + enc(name) + "/status", null);
    }

    public JsonNode config(String connectUrl, String name) {
        return request(connectUrl, "GET",
                "/connectors/" + enc(name) + "/config", null);
    }

    public JsonNode create(String connectUrl, String body) {
        return request(connectUrl, "POST", "/connectors", body);
    }

    public JsonNode updateConfig(String connectUrl, String name, String body) {
        return request(connectUrl, "PUT",
                "/connectors/" + enc(name) + "/config", body);
    }

    public JsonNode restart(String connectUrl, String name) {
        return request(connectUrl, "POST",
                "/connectors/" + enc(name)
                        + "/restart?includeTasks=true&onlyFailed=false", null);
    }

    public JsonNode pause(String connectUrl, String name) {
        return request(connectUrl, "PUT",
                "/connectors/" + enc(name) + "/pause", null);
    }

    public JsonNode resume(String connectUrl, String name) {
        return request(connectUrl, "PUT",
                "/connectors/" + enc(name) + "/resume", null);
    }

    public JsonNode delete(String connectUrl, String name) {
        return request(connectUrl, "DELETE",
                "/connectors/" + enc(name), null);
    }

    private JsonNode request(String connectUrl, String method, String path,
            String body) {
        HttpRequest.Builder builder = HttpRequest
                .newBuilder(URI.create(base(connectUrl) + path))
                .timeout(Duration.ofSeconds(10));
        HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        switch (method) {
            case "GET" -> builder.GET();
            case "DELETE" -> builder.DELETE();
            case "POST" -> builder.header("Content-Type", "application/json")
                    .POST(payload);
            case "PUT" -> builder.header("Content-Type", "application/json")
                    .PUT(payload);
            default -> throw new AdminApiException(500, "INTERNAL",
                    "Unsupported method " + method);
        }

        HttpResponse<String> response;
        try {
            response = HTTP.send(builder.build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new AdminApiException(502, "UNREACHABLE",
                    "Could not reach Kafka Connect at " + connectUrl);
        }
        if (response.statusCode() >= 400) {
            throw new AdminApiException(
                    response.statusCode() == 404 ? 404 : 502,
                    response.statusCode() == 404 ? "NOT_FOUND" : "BAD_REQUEST",
                    "Kafka Connect returned " + response.statusCode() + ": "
                            + summarise(response.body()));
        }
        try {
            String json = response.body();
            return json == null || json.isBlank()
                    ? mapper.createObjectNode()
                    : mapper.readTree(json);
        } catch (Exception e) {
            return mapper.createObjectNode();
        }
    }

    private String summarise(String body) {
        if (body == null || body.isBlank()) {
            return "(no detail)";
        }
        try {
            JsonNode node = mapper.readTree(body);
            if (node.has("message")) {
                return node.get("message").asText();
            }
        } catch (Exception ignored) {
            // not JSON — fall through
        }
        return body.length() > 200 ? body.substring(0, 200) : body;
    }

    private static String base(String connectUrl) {
        String trimmed = connectUrl.trim();
        return trimmed.endsWith("/")
                ? trimmed.substring(0, trimmed.length() - 1)
                : trimmed;
    }

    private static String enc(String name) {
        return URLEncoder.encode(name, StandardCharsets.UTF_8);
    }
}
