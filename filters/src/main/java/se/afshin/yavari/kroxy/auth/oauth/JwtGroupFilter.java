package se.afshin.yavari.kroxy.auth.oauth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.afshin.yavari.kroxy.auth.JwtGroupStore;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Intercepts SASL_AUTHENTICATE for OAUTHBEARER, parses the JWT from the auth bytes,
 * and stores the group claims in JwtGroupStore keyed by the JWT sub claim.
 * JwtGroupSaslSubjectBuilder reads from that store to build a rich Subject.
 * JWT signature is NOT re-verified here — OauthBearerValidation handles that.
 */
class JwtGroupFilter implements SaslAuthenticateRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(JwtGroupFilter.class);
    private static final String OAUTHBEARER = "OAUTHBEARER";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String groupsClaim;

    JwtGroupFilter(String groupsClaim) {
        this.groupsClaim = groupsClaim;
    }

    @Override
    public boolean shouldHandleSaslAuthenticateRequest(short apiVersion) {
        return true;
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(
            short apiVersion,
            RequestHeaderData header,
            SaslAuthenticateRequestData request,
            FilterContext context) {

        try {
            String saslData = new String(request.authBytes(), StandardCharsets.UTF_8);
            if (saslData.contains("auth=Bearer ")) {
                String jwt = extractJwt(saslData);
                if (jwt != null) {
                    parseAndStore(jwt);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to extract JWT groups from SASL auth bytes", e);
        }

        return context.forwardRequest(header, request);
    }

    private static String extractJwt(String saslData) {
        int start = saslData.indexOf("auth=Bearer ");
        if (start < 0) return null;
        start += "auth=Bearer ".length();
        int end = saslData.indexOf('', start);
        return end < 0 ? saslData.substring(start) : saslData.substring(start, end);
    }

    private void parseAndStore(String jwt) throws Exception {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) return;

        byte[] payloadBytes = Base64.getUrlDecoder().decode(padBase64(parts[1]));
        JsonNode payload = MAPPER.readTree(payloadBytes);

        JsonNode subNode = payload.get("sub");
        if (subNode == null) return;
        String sub = subNode.asText();

        Set<String> groups = new HashSet<>();
        JsonNode claimNode = resolveClaim(payload, groupsClaim);
        if (claimNode != null && claimNode.isArray()) {
            claimNode.forEach(g -> groups.add(g.asText()));
        }

        JwtGroupStore.put(sub, groups);

        // Cache preferred_username (Keycloak's human-readable name) for audit display.
        JsonNode unameNode = payload.get("preferred_username");
        if (unameNode != null && !unameNode.isNull()) {
            JwtGroupStore.putUsername(sub, unameNode.asText());
        }
    }

    private static JsonNode resolveClaim(JsonNode root, String claimPath) {
        JsonNode node = root;
        for (String part : claimPath.split("\\.")) {
            if (node == null) return null;
            node = node.get(part);
        }
        return node;
    }

    private static String padBase64(String s) {
        return s + "=".repeat((4 - s.length() % 4) % 4);
    }
}
