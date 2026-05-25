package se.afshin.yavari.kroxy.audit;

import java.io.Closeable;
import java.io.IOException;

/**
 * Emits {@link AuditEvent}s to one or more sinks. Implementations must be
 * non-blocking on the calling thread — Kroxylicious filter and HTTP request
 * threads must never stall on an audit sink.
 */
public interface AuditEmitter extends Closeable {

    void emit(AuditEvent event);

    @Override
    default void close() throws IOException {}
}
