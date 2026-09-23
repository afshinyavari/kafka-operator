package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * Shared HTTP plumbing for the registry clients: JDK {@link HttpClient} with an optional
 * mTLS {@link SSLContext}, {@code Authorization} from an
 * {@link ApicurioClient.AuthProvider}, and a single retry after a 401/403 once the
 * provider has refreshed its credentials. Failures are classified into
 * {@link RegistryException.Kind}s: I/O errors and 408/429/5xx are transient, other
 * non-2xx statuses permanent. Only the subclasses' id lookups produce
 * {@link RegistryException.Kind#NOT_FOUND}.
 */
abstract class RestRegistryClient implements SchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(RestRegistryClient.class);
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected final String baseUrl;
    private final HttpClient http;
    private final ApicurioClient.AuthProvider auth;

    protected RestRegistryClient(String baseUrl, ApicurioClient.AuthProvider auth, SSLContext ssl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.auth = auth != null ? auth : ApicurioClient.AuthProvider.none();
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10));
        if (ssl != null) b.sslContext(ssl);
        this.http = b.build();
    }

    protected JsonNode getJson(String path) throws RegistryException {
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
        if (resp.statusCode() / 100 != 2) throw failure("GET", path, resp);
        return parse(resp.body(), path);
    }

    /** Like {@link #getJson} but maps a 404 to {@code null}. The caller decides whether
     *  that null is a {@link RegistryException.Kind#NOT_FOUND} (the record's own id) or a
     *  config error (a target endpoint that doesn't exist). */
    protected JsonNode getJsonOrNull(String path) throws RegistryException {
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() / 100 != 2) throw failure("GET", path, resp);
        return parse(resp.body(), path);
    }

    protected byte[] getRaw(String path) throws RegistryException {
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
        if (resp.statusCode() / 100 != 2) throw failure("GET", path, resp);
        return resp.body();
    }

    /** Like {@link #getRaw} but maps a 404 to {@code null}. */
    protected byte[] getRawOrNull(String path) throws RegistryException {
        HttpResponse<byte[]> resp = exchange("GET", path, null, null);
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() / 100 != 2) throw failure("GET", path, resp);
        return resp.body();
    }

    /** POSTs and returns the parsed JSON body on 2xx; throws with the body on failure. */
    protected JsonNode postJson(String path, byte[] body, Map<String, String> headers)
            throws RegistryException {
        HttpResponse<byte[]> resp = exchange("POST", path, body, headers);
        if (resp.statusCode() / 100 != 2) throw failure("POST", path, resp);
        return parse(resp.body(), path);
    }

    /** POSTs and returns the parsed body on 2xx, {@code null} on 404. */
    protected JsonNode postJsonOrNull(String path, byte[] body, Map<String, String> headers)
            throws RegistryException {
        HttpResponse<byte[]> resp = exchange("POST", path, body, headers);
        if (resp.statusCode() == 404) return null;
        if (resp.statusCode() / 100 != 2) throw failure("POST", path, resp);
        return parse(resp.body(), path);
    }

    /** Non-2xx → exception classified by status: 408/429/5xx transient, other 4xx permanent. */
    private RegistryException failure(String method, String path, HttpResponse<byte[]> resp) {
        String body = resp.body() == null ? "" : new String(resp.body(), StandardCharsets.UTF_8);
        String msg = method + " " + baseUrl + path + " → " + resp.statusCode();
        if (!body.isBlank()) msg += " body=" + abbreviate(body);
        return RegistryException.forStatus(resp.statusCode(), msg);
    }

    private static String abbreviate(String s) {
        return s.length() <= 500 ? s : s.substring(0, 500) + "…";
    }

    private static JsonNode parse(byte[] body, String path) throws RegistryException {
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new RegistryException("Failed to parse JSON from " + path, e);
        }
    }

    /** Sends a request with the current auth header; on a 401/403 refreshes the credentials
     *  (if refreshable) and retries exactly once. */
    private HttpResponse<byte[]> exchange(String method, String path, byte[] body,
                                          Map<String, String> headers) throws RegistryException {
        HttpResponse<byte[]> resp = send(method, path, body, headers);
        if ((resp.statusCode() == 401 || resp.statusCode() == 403) && auth.refresh()) {
            LOG.debug("Auth rejected ({}) on {} {} — refreshed credentials, retrying",
                    resp.statusCode(), method, path);
            resp = send(method, path, body, headers);
        }
        return resp;
    }

    private HttpResponse<byte[]> send(String method, String path, byte[] body,
                                      Map<String, String> headers) throws RegistryException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30));
        if (headers != null) headers.forEach(b::header);
        if ("POST".equals(method)) {
            b.POST(BodyPublishers.ofByteArray(body));
        } else {
            b.GET();
        }
        String header = auth.header();
        if (header != null) b.header("Authorization", header);
        try {
            return http.send(b.build(), BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw RegistryException.transientError("Interrupted: " + method + " " + baseUrl + path, e);
        } catch (Exception e) {
            // Connection refused, DNS, TLS handshake, read timeout — all worth a retry.
            throw RegistryException.transientError("HTTP request failed: " + method + " " + baseUrl + path
                    + ": " + e.getMessage(), e);
        }
    }

    protected static String textOrNull(JsonNode n) {
        return n == null || n.isNull() ? null : n.asText();
    }

    protected static String textOrDefault(JsonNode n, String fallback) {
        return n == null || n.isNull() ? fallback : n.asText();
    }

    protected static String pathSegment(String s) {
        return s.replace("/", "%2F");
    }

    @Override
    public void close() {
        // JDK HttpClient has no explicit close before JDK 21; rely on GC.
    }
}
