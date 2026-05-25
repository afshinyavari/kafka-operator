package se.afshin.yavari.kroxy.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default audit sink. Writes each event as a one-line JSON record at INFO on
 * the dedicated SLF4J logger {@code kafka-audit}. Log shippers (Fluent Bit,
 * Vector, Loki Promtail, ...) can route this channel separately from the
 * normal application logs.
 */
public final class StdoutAuditEmitter implements AuditEmitter {

    private static final Logger LOG = LoggerFactory.getLogger("kafka-audit");

    @Override
    public void emit(AuditEvent event) {
        LOG.info(AuditEventJson.toJson(event));
    }
}
