package se.afshin.yavari.kafka.ui.kafka;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(value = 3, unit = TimeUnit.SECONDS)
class JwtCallbackHandlerTest {

    @Test
    void parsesPrincipalAndExpiryFromJwt() throws IOException {
        String jwt = makeJwt("""
                {"sub":"alice-uuid","preferred_username":"alice","exp":2000000000,"iat":1000000000}
                """);

        OAuthBearerToken tok = JwtCallbackHandler.parse(jwt);

        assertThat(tok.value()).isEqualTo(jwt);
        assertThat(tok.principalName()).isEqualTo("alice");
        assertThat(tok.lifetimeMs()).isEqualTo(2_000_000_000L * 1000L);
        assertThat(tok.startTimeMs()).isEqualTo(1_000_000_000L * 1000L);
    }

    @Test
    void fallsBackToSubWhenPreferredUsernameMissing() throws IOException {
        String jwt = makeJwt("""
                {"sub":"alice-uuid","exp":2000000000}
                """);
        assertThat(JwtCallbackHandler.parse(jwt).principalName()).isEqualTo("alice-uuid");
    }

    @Test
    void handlesScopeStringClaim() throws IOException {
        String jwt = makeJwt("""
                {"sub":"x","exp":1,"scope":"openid profile email"}
                """);
        assertThat(JwtCallbackHandler.parse(jwt).scope())
                .containsExactlyInAnyOrder("openid", "profile", "email");
    }

    @Test
    void rejectsNonJwsString() {
        assertThatThrownBy(() -> JwtCallbackHandler.parse("not-a-jwt"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void configureExtractsRawTokenFromJaas() {
        JwtCallbackHandler h = new JwtCallbackHandler();
        h.configure(Map.of(), "OAUTHBEARER", List.of(jaas(Map.of("rawToken", "abc.def.ghi"))));
        // No exception → configured.
    }

    @Test
    void configureRequiresRawToken() {
        JwtCallbackHandler h = new JwtCallbackHandler();
        assertThatThrownBy(() -> h.configure(Map.of(), "OAUTHBEARER", List.of(jaas(Map.of()))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rawToken");
    }

    @Test
    void handleProducesTokenForCallback() throws Exception {
        JwtCallbackHandler h = new JwtCallbackHandler();
        String jwt = makeJwt("""
                {"sub":"bob-uuid","preferred_username":"bob","exp":3000000000}
                """);
        h.configure(Map.of(), "OAUTHBEARER", List.of(jaas(Map.of("rawToken", jwt))));

        OAuthBearerTokenCallback cb = new OAuthBearerTokenCallback();
        h.handle(new Callback[]{cb});

        assertThat(cb.token()).isNotNull();
        assertThat(cb.token().principalName()).isEqualTo("bob");
    }

    @Test
    void rejectsUnknownCallback() {
        JwtCallbackHandler h = new JwtCallbackHandler();
        h.configure(Map.of(), "OAUTHBEARER",
                List.of(jaas(Map.of("rawToken", makeJwt("{\"sub\":\"x\",\"exp\":1}")))));
        assertThatThrownBy(() -> h.handle(new Callback[]{new Callback() {}}))
                .isInstanceOf(UnsupportedCallbackException.class);
    }

    @Test
    void concurrentHandlers_doNotCrossTokens() throws Exception {
        // Two handler instances configured with different JWTs must each
        // return their own token under parallel invocation. This is the
        // guard against the JAAS-subject collision risk called out in the plan.
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            String aliceJwt = makeJwt("{\"sub\":\"alice\",\"preferred_username\":\"alice\",\"exp\":3000000000}");
            String bobJwt = makeJwt("{\"sub\":\"bob\",\"preferred_username\":\"bob\",\"exp\":3000000000}");

            JwtCallbackHandler aliceH = new JwtCallbackHandler();
            aliceH.configure(Map.of(), "OAUTHBEARER", List.of(jaas(Map.of("rawToken", aliceJwt))));
            JwtCallbackHandler bobH = new JwtCallbackHandler();
            bobH.configure(Map.of(), "OAUTHBEARER", List.of(jaas(Map.of("rawToken", bobJwt))));

            CompletableFuture<?>[] tasks = new CompletableFuture[100];
            for (int i = 0; i < tasks.length; i++) {
                final boolean alice = i % 2 == 0;
                tasks[i] = CompletableFuture.runAsync(() -> {
                    try {
                        OAuthBearerTokenCallback cb = new OAuthBearerTokenCallback();
                        (alice ? aliceH : bobH).handle(new Callback[]{cb});
                        assertThat(cb.token().principalName()).isEqualTo(alice ? "alice" : "bob");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, pool);
            }
            CompletableFuture.allOf(tasks).get();
        } finally {
            pool.shutdown();
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static AppConfigurationEntry jaas(Map<String, ?> opts) {
        return new AppConfigurationEntry("org.apache.kafka.x.LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                new HashMap<>(opts));
    }

    private static String makeJwt(String payloadJson) {
        // Three-segment compact JWS; we don't sign because the handler does not verify.
        String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
        String payload = base64Url(payloadJson.trim());
        return header + "." + payload + ".sig";
    }

    private static String base64Url(String s) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes());
    }
}
