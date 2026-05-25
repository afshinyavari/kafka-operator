package se.afshin.yavari.kroxy.audit;

import java.time.Instant;

/**
 * One audit record emitted per Kafka request (Kroxylicious) or HTTP request
 * (Apicurio rbac-proxy). Fields are deliberately flat so the JSON line is
 * trivially queryable in Loki / ELK without an extra parser.
 *
 * <p>{@code principal} is {@code user:<CN-or-sub>} for an authenticated caller
 * or {@code group:<g>} when the request only carries a group claim.
 * {@code op} is the verb the component understands — {@code PRODUCE},
 * {@code FETCH}, {@code CREATE_TOPICS} for Kafka; {@code READ}, {@code WRITE},
 * {@code DELETE} for the rbac-proxy. {@code decision} is {@code allow},
 * {@code deny}, or {@code error}.
 */
public record AuditEvent(
        Instant ts,
        String principal,
        String op,
        String resource,
        String decision,
        long latencyMs,
        String correlationId) {
}
