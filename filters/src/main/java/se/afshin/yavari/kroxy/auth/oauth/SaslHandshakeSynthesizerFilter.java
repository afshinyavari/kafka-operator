package se.afshin.yavari.kroxy.auth.oauth;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateResponseFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeResponseFilter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Synthesizes SASL exchange responses for a PLAINTEXT upstream broker.
 *
 * SASL_HANDSHAKE: the PLAINTEXT broker returns no supported mechanisms.
 * We rewrite the response to advertise OAUTHBEARER so the client proceeds.
 *
 * SASL_AUTHENTICATE: after OauthBearerValidationFilter validates the JWT it
 * forwards the SASL_AUTHENTICATE to the upstream. The PLAINTEXT broker has
 * auto-authenticated (ANONYMOUS) and rejects it with ILLEGAL_SASL_STATE.
 * We intercept that error and synthesize a success response — the JWT was
 * already validated by the time this callback is reached.
 */
class SaslHandshakeSynthesizerFilter implements SaslHandshakeResponseFilter, SaslAuthenticateResponseFilter {

    @Override
    public CompletionStage<ResponseFilterResult> onSaslHandshakeResponse(
            short apiVersion,
            ResponseHeaderData header,
            SaslHandshakeResponseData response,
            FilterContext context) {

        response.setErrorCode(Errors.NONE.code());
        response.setMechanisms(List.of("OAUTHBEARER"));
        return context.forwardResponse(header, response);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onSaslAuthenticateResponse(
            short apiVersion,
            ResponseHeaderData header,
            SaslAuthenticateResponseData response,
            FilterContext context) {

        // Synthesize success: the PLAINTEXT broker rejects SASL_AUTHENTICATE with
        // ILLEGAL_SASL_STATE because it considers itself already authenticated.
        // The JWT was validated upstream by OauthBearerValidationFilter, so it
        // is safe to override the error here.
        response.setErrorCode(Errors.NONE.code());
        response.setErrorMessage(null);
        response.setAuthBytes(new byte[0]);
        return context.forwardResponse(header, response);
    }
}
