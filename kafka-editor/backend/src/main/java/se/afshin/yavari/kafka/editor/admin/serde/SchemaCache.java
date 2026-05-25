package se.afshin.yavari.kafka.editor.admin.serde;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * Caches schema content fetched from Apicurio by global / content id. Keyed by
 * registry URL + id so multiple clusters do not collide. Misses are cached too,
 * so a record stream with no registry does not hammer the network.
 */
@ApplicationScoped
public class SchemaCache {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final Map<String, Optional<String>> cache = new ConcurrentHashMap<>();

    /** Raw schema content for an Apicurio global id, or empty if unavailable. */
    public Optional<String> byId(String registryUrl, long id) {
        if (registryUrl == null || registryUrl.isBlank()) {
            return Optional.empty();
        }
        return cache.computeIfAbsent(registryUrl.trim() + "#" + id,
                key -> fetch(registryUrl.trim(), id));
    }

    private Optional<String> fetch(String registryUrl, long id) {
        String base = registryUrl.endsWith("/")
                ? registryUrl.substring(0, registryUrl.length() - 1)
                : registryUrl;
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(
                                    base + "/apis/registry/v2/ids/globalIds/" + id))
                            .GET()
                            .timeout(Duration.ofSeconds(8))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200
                    ? Optional.of(response.body())
                    : Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
