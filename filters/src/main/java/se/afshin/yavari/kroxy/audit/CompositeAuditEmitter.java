package se.afshin.yavari.kroxy.audit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Emits to every wrapped emitter in order. The stdout sink is always first so
 * that — even when a remote sink is misconfigured — the audit trail still lands
 * in the container's log stream.
 */
public final class CompositeAuditEmitter implements AuditEmitter {

    private final List<AuditEmitter> delegates;

    public CompositeAuditEmitter(List<AuditEmitter> delegates) {
        this.delegates = List.copyOf(Objects.requireNonNull(delegates, "delegates"));
    }

    @Override
    public void emit(AuditEvent event) {
        for (AuditEmitter d : delegates) {
            d.emit(event);
        }
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (AuditEmitter d : delegates) {
            try {
                d.close();
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        if (first != null) throw first;
    }
}
