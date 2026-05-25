package se.afshin.yavari.kroxy.audit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async Kafka sink for audit events. On send failure (network blip, topic
 * missing, ACL not yet propagated, ...) the event is delegated to the supplied
 * {@code fallback} emitter — typically {@link StdoutAuditEmitter} — so an audit
 * record is never silently lost. A warning is logged at most once per
 * {@link #WARN_THROTTLE_MILLIS} so a long outage does not spam the proxy log.
 *
 * <p>The wrapped producer <b>must</b> be configured with {@code max.block.ms=0}
 * (the {@link AuditEmitters} factory enforces this); otherwise a full
 * accumulator would freeze the request thread.
 */
public final class KafkaAuditEmitter implements AuditEmitter {

    static final long WARN_THROTTLE_MILLIS = 60_000L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuditEmitter.class);

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final AuditEmitter fallback;
    private final AtomicLong lastWarn = new AtomicLong(0);

    public KafkaAuditEmitter(Producer<byte[], byte[]> producer, String topic, AuditEmitter fallback) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.fallback = Objects.requireNonNull(fallback, "fallback");
    }

    @Override
    public void emit(AuditEvent event) {
        byte[] payload = AuditEventJson.toJson(event).getBytes(StandardCharsets.UTF_8);
        try {
            producer.send(new ProducerRecord<>(topic, null, payload), (meta, err) -> {
                if (err != null) {
                    fallback.emit(event);
                    rateLimitedWarn(err);
                }
            });
        } catch (Exception sync) {
            // max.block.ms=0 surfaces a full buffer as a synchronous exception; treat the
            // same as an async failure — fall back to stdout, warn, never re-throw.
            fallback.emit(event);
            rateLimitedWarn(sync);
        }
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (Exception e) {
            LOG.warn("Error closing audit Kafka producer", e);
        }
    }

    private void rateLimitedWarn(Throwable err) {
        long now = System.currentTimeMillis();
        long previous = lastWarn.get();
        if (now - previous < WARN_THROTTLE_MILLIS) return;
        if (!lastWarn.compareAndSet(previous, now)) return;
        LOG.warn("Failed to ship audit event to Kafka topic {} — falling back to stdout: {}",
                topic, err.getMessage());
    }
}
