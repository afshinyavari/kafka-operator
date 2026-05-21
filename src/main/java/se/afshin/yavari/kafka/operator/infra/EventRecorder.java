package se.afshin.yavari.kafka.operator.infra;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

/**
 * Emits K8s Events for reconciliation milestones — the ones that show up under
 * {@code kubectl describe kafkacluster …}. Keep call sites sparse: reserve for
 * phase transitions, roll boundaries, secret-rotation triggers. Continuous
 * progress messages belong in logs (via {@link ReconcileContext}), not events.
 */
@ApplicationScoped
public class EventRecorder {

    private static final Logger LOG = Logger.getLogger(EventRecorder.class);

    public enum Type {
        NORMAL("Normal"), WARNING("Warning");
        public final String value;
        Type(String v) { this.value = v; }
    }

    @Inject KubernetesClient client;

    /** Records a one-off event. Errors talking to the API server are swallowed —
     *  events are best-effort and must never break the reconcile. */
    public void emit(HasMetadata involvedObject, Type type, String reason, String message) {
        String ns = involvedObject.getMetadata().getNamespace();
        String now = Instant.now().toString();
        Event event = new EventBuilder()
                .withNewMetadata()
                    .withName(eventName(involvedObject))
                    .withNamespace(ns)
                .endMetadata()
                .withInvolvedObject(ref(involvedObject))
                .withType(type.value)
                .withReason(reason)
                .withMessage(message)
                .withFirstTimestamp(now)
                .withLastTimestamp(now)
                .withCount(1)
                .withSource(new EventSource("kafka-operator", null))
                .build();
        try {
            client.v1().events().inNamespace(ns).resource(event).create();
        } catch (Exception e) {
            LOG.debugf("Event emit skipped for %s/%s %s: %s",
                    ns, involvedObject.getMetadata().getName(), reason, e.getMessage());
        }
    }

    private String eventName(HasMetadata cr) {
        // Events are immutable in the API; use a fresh suffix so each emit is its own
        // resource. The base name keeps `kubectl get events` grouped.
        return cr.getMetadata().getName() + "."
                + Long.toHexString(System.currentTimeMillis())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private ObjectReference ref(HasMetadata cr) {
        ObjectReference r = new ObjectReference();
        r.setApiVersion(cr.getApiVersion());
        r.setKind(cr.getKind());
        r.setName(cr.getMetadata().getName());
        r.setNamespace(cr.getMetadata().getNamespace());
        r.setUid(cr.getMetadata().getUid());
        r.setResourceVersion(cr.getMetadata().getResourceVersion());
        return r;
    }
}
