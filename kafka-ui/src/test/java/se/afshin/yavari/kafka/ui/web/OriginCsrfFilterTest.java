package se.afshin.yavari.kafka.ui.web;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OriginCsrfFilterTest {

    private final OriginCsrfFilter filter = newFilter(true, "");

    @Test
    void passes_safeMethods_withoutChecking() throws Exception {
        var ctx = mockCtx("GET", "https://evil.example", "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    @Test
    void passes_post_whenOriginMatchesHost() throws Exception {
        var ctx = mockCtx("POST", "https://kafka-ui.kafka.svc", "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    @Test
    void passes_post_whenOriginMatchesHostWithPort() throws Exception {
        var ctx = mockCtx("POST", "https://kafka-ui.kafka.svc:8443", "kafka-ui.kafka.svc:8443");
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    @Test
    void rejects_post_whenOriginMismatch() throws Exception {
        var ctx = mockCtx("POST", "https://evil.example", "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNotNull();
        assertThat(aborted.get().getStatus()).isEqualTo(403);
    }

    @Test
    void passes_post_withoutOriginHeader_relyOnSameSiteCookie() throws Exception {
        var ctx = mockCtx("POST", null, "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    @Test
    void rejects_post_withMissingHostHeader() throws Exception {
        var ctx = mockCtx("POST", "https://kafka-ui.kafka.svc", null);
        var aborted = captureAbort(ctx);
        filter.filter(ctx);
        assertThat(aborted.get()).isNotNull();
        assertThat(aborted.get().getStatus()).isEqualTo(403);
    }

    @Test
    void passes_post_whenOriginInAllowList() throws Exception {
        OriginCsrfFilter allowListed = newFilter(true, "https://embed.example");
        var ctx = mockCtx("POST", "https://embed.example", "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        allowListed.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    @Test
    void passes_post_whenDisabled() throws Exception {
        OriginCsrfFilter disabled = newFilter(false, "");
        var ctx = mockCtx("POST", "https://evil.example", "kafka-ui.kafka.svc");
        var aborted = captureAbort(ctx);
        disabled.filter(ctx);
        assertThat(aborted.get()).isNull();
    }

    /* helpers */

    private static OriginCsrfFilter newFilter(boolean enabled, String allowedOrigins) {
        OriginCsrfFilter f = new OriginCsrfFilter();
        f.configure(enabled, allowedOrigins);
        return f;
    }

    private static ContainerRequestContext mockCtx(String method, String origin, String host) {
        ContainerRequestContext ctx = mock(ContainerRequestContext.class);
        when(ctx.getMethod()).thenReturn(method);
        when(ctx.getHeaderString("Origin")).thenReturn(origin);
        when(ctx.getHeaderString("Referer")).thenReturn(null);
        when(ctx.getHeaderString("Host")).thenReturn(host);
        return ctx;
    }

    private static AtomicReference<Response> captureAbort(ContainerRequestContext ctx) {
        AtomicReference<Response> ref = new AtomicReference<>();
        doAnswer(inv -> {
            ref.set(inv.getArgument(0, Response.class));
            return null;
        }).when(ctx).abortWith(any(Response.class));
        return ref;
    }
}
