package se.afshin.yavari.kafka.smt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import se.afshin.yavari.kafka.smt.ApicurioClient.ApicurioException;

/**
 * Fetches and caches OAuth2 access tokens via the <em>client-credentials</em> grant,
 * refreshing them before expiry. Thread-safe.
 *
 * <p>{@link ApicurioClient} uses this to authenticate to an Apicurio registry that sits
 * behind an OIDC-gated proxy — specifically the operator's {@code apicurio-rbac-proxy},
 * which rejects unauthenticated writes. The SMT runs inside a long-lived MirrorMaker2
 * worker, so a static token is unworkable (it expires within minutes); this provider
 * re-fetches a fresh token whenever the cached one is within {@link #EXPIRY_MARGIN_SECONDS}
 * of expiring, or when {@link #invalidate()} is called after a 401/403.
 */
public class OAuthTokenProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenProvider.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Re-fetch this many seconds before the token's stated expiry, to absorb clock skew
     *  and request latency. */
    private static final long EXPIRY_MARGIN_SECONDS = 30;

    private final String tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String scope;        // nullable — omitted from the request when blank
    private final HttpClient http;

    private String cachedToken;
    private Instant expiresAt = Instant.EPOCH;

    public OAuthTokenProvider(String tokenUrl, String clientId, String clientSecret, String scope) {
        if (tokenUrl == null || tokenUrl.isBlank()) {
            throw new IllegalArgumentException("OAuth token URL must not be blank");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("OAuth client id must not be blank");
        }
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret == null ? "" : clientSecret;
        this.scope = scope;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /** Returns a valid access token, fetching or refreshing as needed. */
    public synchronized String token() throws ApicurioException {
        if (cachedToken != null && Instant.now().isBefore(expiresAt)) {
            return cachedToken;
        }
        return fetch();
    }

    /** Drops the cached token so the next {@link #token()} re-fetches. Called after a
     *  401/403 in case the token was revoked or the keys rotated. */
    public synchronized void invalidate() {
        cachedToken = null;
        expiresAt = Instant.EPOCH;
    }

    private String fetch() throws ApicurioException {
        StringBuilder form = new StringBuilder()
                .append("grant_type=client_credentials")
                .append("&client_id=").append(enc(clientId))
                .append("&client_secret=").append(enc(clientSecret));
        if (scope != null && !scope.isBlank()) {
            form.append("&scope=").append(enc(scope));
        }
        HttpRequest req = HttpRequest.newBuilder(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30))
                .POST(BodyPublishers.ofString(form.toString()))
                .build();
        HttpResponse<byte[]> resp;
        try {
            resp = http.send(req, BodyHandlers.ofByteArray());
        } catch (Exception e) {
            throw new ApicurioException("OAuth token request failed: " + tokenUrl, e);
        }
        if (resp.statusCode() / 100 != 2) {
            throw new ApicurioException("OAuth token endpoint " + tokenUrl + " → " + resp.statusCode()
                    + " body=" + new String(resp.body(), StandardCharsets.UTF_8));
        }
        try {
            JsonNode json = MAPPER.readTree(resp.body());
            String token = json.path("access_token").asText(null);
            if (token == null || token.isBlank()) {
                throw new ApicurioException("OAuth token response missing access_token");
            }
            long expiresIn = json.path("expires_in").asLong(300);
            this.cachedToken = token;
            this.expiresAt = Instant.now().plusSeconds(Math.max(1, expiresIn - EXPIRY_MARGIN_SECONDS));
            LOG.info("OAuth token acquired from {} (expires_in={}s)", tokenUrl, expiresIn);
            return token;
        } catch (ApicurioException e) {
            throw e;
        } catch (Exception e) {
            throw new ApicurioException("Failed to parse OAuth token response from " + tokenUrl, e);
        }
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }
}
