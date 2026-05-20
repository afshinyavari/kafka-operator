package se.afshin.yavari.kafka.ui.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * SASL/OAUTHBEARER login callback handler that hands back a pre-acquired JWT.
 *
 * <p>The token is passed in via the {@code sasl.jaas.config} line as a
 * {@code rawToken} option. This lets us safely propagate the logged-in user's
 * bearer token from the HTTP request context into kafka-clients without
 * relying on a {@code ThreadLocal} (which would not survive the hand-off to
 * AdminClient's internal IO thread). Each AdminClient instance gets its own
 * callback-handler instance whose token is captured at {@link #configure}.
 *
 * <p>No signature verification happens here — the KafkaProxy's
 * {@code oauth-bearer-validation} filter validates the JWT against the JWKS
 * before any operation reaches the broker, so this code only needs to decode
 * the payload claims to populate the {@link OAuthBearerToken} contract.
 */
public class JwtCallbackHandler implements AuthenticateCallbackHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String rawToken;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism,
                          List<AppConfigurationEntry> jaasConfigEntries) {
        if (jaasConfigEntries == null || jaasConfigEntries.isEmpty()) {
            throw new IllegalStateException("No JAAS entries for OAUTHBEARER");
        }
        this.rawToken = Objects.toString(
                jaasConfigEntries.get(0).getOptions().get("rawToken"), null);
        if (rawToken == null || rawToken.isBlank()) {
            throw new IllegalStateException("rawToken option missing from sasl.jaas.config");
        }
    }

    @Override
    public void close() { /* nothing to release */ }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof OAuthBearerTokenCallback otc) {
                otc.token(parse(rawToken));
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }

    static OAuthBearerToken parse(String compact) throws IOException {
        String[] parts = compact.split("\\.");
        if (parts.length < 2) {
            throw new IOException("Token is not a JWS compact serialisation");
        }
        byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
        JsonNode claims = MAPPER.readTree(payload);

        String principal = firstNonBlank(
                claims.path("preferred_username").asText(null),
                claims.path("sub").asText(null));
        long lifetimeMs = claims.path("exp").asLong(0L) * 1000L;
        long startMs = claims.path("iat").asLong(0L) * 1000L;
        Set<String> scopes = parseScopes(claims);

        return new OAuthBearerToken() {
            @Override public String value() { return compact; }
            @Override public Set<String> scope() { return scopes; }
            @Override public long lifetimeMs() { return lifetimeMs; }
            @Override public String principalName() { return principal; }
            @Override public Long startTimeMs() { return startMs == 0 ? null : startMs; }
        };
    }

    private static Set<String> parseScopes(JsonNode claims) {
        JsonNode scope = claims.path("scope");
        if (scope.isMissingNode() || scope.isNull()) return Collections.emptySet();
        Set<String> out = new HashSet<>();
        if (scope.isTextual()) {
            for (String s : scope.asText().split("\\s+")) {
                if (!s.isBlank()) out.add(s);
            }
            return out;
        }
        if (scope.isArray()) {
            Iterator<JsonNode> it = scope.elements();
            while (it.hasNext()) {
                JsonNode n = it.next();
                if (n.isTextual()) out.add(n.asText());
            }
        }
        return out;
    }

    private static String firstNonBlank(String... s) {
        for (String x : s) if (x != null && !x.isBlank()) return x;
        return "unknown";
    }
}
