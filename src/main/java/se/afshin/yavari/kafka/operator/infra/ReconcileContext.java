package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.HasMetadata;
import org.jboss.logmanager.MDC;

import java.util.UUID;

/**
 * MDC scope for one reconcile invocation. Use try-with-resources at the top of every
 * {@code Reconciler.reconcile()} so every log line emitted while the call runs carries
 * the {@code reconcile-id}, {@code kind}, {@code namespace}, and {@code name} keys.
 *
 * <p>JSON logging (enabled in the prod profile via {@code quarkus.log.console.json=true})
 * promotes each MDC key to a top-level field, so log aggregation can group by
 * {@code reconcile-id} or filter by CR kind/name.
 *
 * <pre>{@code
 * try (var ignored = ReconcileContext.scope(cr)) {
 *     // reconcile body
 * }
 * }</pre>
 */
public final class ReconcileContext implements AutoCloseable {

    private final String reconcileId;
    private final String kind;
    private final String namespace;
    private final String name;

    private ReconcileContext(String kind, String namespace, String name) {
        this.reconcileId = UUID.randomUUID().toString();
        this.kind = kind;
        this.namespace = namespace;
        this.name = name;
        MDC.put("reconcile-id", reconcileId);
        MDC.put("kind", kind);
        MDC.put("namespace", namespace);
        MDC.put("name", name);
    }

    public static ReconcileContext scope(HasMetadata cr) {
        return new ReconcileContext(cr.getKind(),
                cr.getMetadata().getNamespace(),
                cr.getMetadata().getName());
    }

    /** Convenience for the synthesised CR types (KafkaProxy / ApicurioRegistry after Wave 4)
     *  that aren't fabric8 {@code HasMetadata}. */
    public static ReconcileContext scope(String kind, String namespace, String name) {
        return new ReconcileContext(kind, namespace, name);
    }

    public String id() { return reconcileId; }

    @Override
    public void close() {
        MDC.remove("reconcile-id");
        MDC.remove("kind");
        MDC.remove("namespace");
        MDC.remove("name");
    }
}
