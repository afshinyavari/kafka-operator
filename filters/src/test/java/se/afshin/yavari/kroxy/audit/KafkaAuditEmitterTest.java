package se.afshin.yavari.kroxy.audit;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAuditEmitterTest {

    private static AuditEvent sampleEvent() {
        return new AuditEvent(Instant.parse("2026-05-25T12:00:00Z"),
                "user:alice", "PRODUCE", "orders", "allow", 5, "corr-1");
    }

    @Test
    void successfulSend_doesNotInvokeFallback() {
        MockProducer<byte[], byte[]> producer = new MockProducer<>(true,
                new ByteArraySerializer(), new ByteArraySerializer());
        CapturingEmitter fallback = new CapturingEmitter();
        KafkaAuditEmitter emitter = new KafkaAuditEmitter(producer, "__audit", fallback);

        emitter.emit(sampleEvent());

        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).topic()).isEqualTo("__audit");
        assertThat(fallback.events).isEmpty();
    }

    @Test
    void asyncFailure_routesToFallback() {
        // autoComplete=false so we can deterministically signal failure.
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false,
                new ByteArraySerializer(), new ByteArraySerializer());
        CapturingEmitter fallback = new CapturingEmitter();
        KafkaAuditEmitter emitter = new KafkaAuditEmitter(producer, "__audit", fallback);

        emitter.emit(sampleEvent());
        producer.errorNext(new TimeoutException("simulated"));

        assertThat(fallback.events).hasSize(1);
        assertThat(fallback.events.get(0).resource()).isEqualTo("orders");
    }

    /** Records every event for assertion. */
    private static final class CapturingEmitter implements AuditEmitter {
        final List<AuditEvent> events = new ArrayList<>();
        @Override public void emit(AuditEvent event) { events.add(event); }
    }
}
