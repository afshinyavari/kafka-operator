package se.afshin.yavari.rbac;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import se.afshin.yavari.kroxy.audit.AuditEmitter;
import se.afshin.yavari.kroxy.audit.AuditEmitters;

import java.io.IOException;

/**
 * CDI wiring for the process-wide {@link AuditEmitter}. Produced once and
 * shared across all {@code ProxyResource} invocations. Reads the
 * {@code KAFKA_AUDIT_*} environment variables (see {@link AuditEmitters}) —
 * when {@code KAFKA_AUDIT_BOOTSTRAP} is unset only the always-on stdout sink
 * is installed.
 */
public class AuditConfig {

    @Produces
    @ApplicationScoped
    public AuditEmitter auditEmitter() {
        return AuditEmitters.fromEnv();
    }

    public void closeAuditEmitter(@Disposes AuditEmitter emitter) throws IOException {
        emitter.close();
    }
}
