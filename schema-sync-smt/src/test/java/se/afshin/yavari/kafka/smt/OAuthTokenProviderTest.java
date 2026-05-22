package se.afshin.yavari.kafka.smt;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link OAuthTokenProvider} against an in-process token endpoint. */
class OAuthTokenProviderTest {

    private HttpServer server;
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private volatile int statusCode = 200;
    private volatile long expiresIn = 300;

    @BeforeEach
    void start() throws IOException {
        tokenRequests.set(0);
        statusCode = 200;
        expiresIn = 300;
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", ex -> {
            int n = tokenRequests.incrementAndGet();
            byte[] body;
            int code = statusCode;
            if (code / 100 == 2) {
                body = ("{\"access_token\":\"tok-" + n + "\",\"expires_in\":" + expiresIn + "}")
                        .getBytes(StandardCharsets.UTF_8);
            } else {
                body = "{\"error\":\"invalid_client\"}".getBytes(StandardCharsets.UTF_8);
            }
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(code, body.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(body); }
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        if (server != null) server.stop(0);
    }

    private OAuthTokenProvider provider() {
        return new OAuthTokenProvider(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/token",
                "mm2-schema-sync", "secret", null);
    }

    @Test
    void fetchesAndCachesToken() throws Exception {
        OAuthTokenProvider p = provider();
        assertThat(p.token()).isEqualTo("tok-1");
        // A second call within the validity window must not re-hit the endpoint.
        assertThat(p.token()).isEqualTo("tok-1");
        assertThat(tokenRequests.get()).isEqualTo(1);
    }

    @Test
    void invalidateForcesRefetch() throws Exception {
        OAuthTokenProvider p = provider();
        assertThat(p.token()).isEqualTo("tok-1");
        p.invalidate();
        assertThat(p.token()).isEqualTo("tok-2");
        assertThat(tokenRequests.get()).isEqualTo(2);
    }

    @Test
    void shortLivedTokenIsRefetched() throws Exception {
        // expires_in below the 30s margin → token is treated as already expired, so every
        // call re-fetches.
        expiresIn = 1;
        OAuthTokenProvider p = provider();
        p.token();
        Thread.sleep(1100);
        p.token();
        assertThat(tokenRequests.get()).isEqualTo(2);
    }

    @Test
    void nonTwoxxResponseThrows() {
        statusCode = 401;
        assertThatThrownBy(() -> provider().token())
                .isInstanceOf(ApicurioClient.ApicurioException.class)
                .hasMessageContaining("401");
    }
}
