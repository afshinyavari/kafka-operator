package se.afshin.yavari.clientapp.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.avro.Event;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.ProducerConfig;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Sends one {@link Event} per interval. Failures are logged and counted, never fatal. */
public final class ProducerRunner {

    private static final Logger LOG = Logger.getLogger(ProducerRunner.class);

    private final ProducerConfig config;
    private final Producer<String, Object> producer;
    private final PayloadGenerator generator;
    private final java.util.function.Consumer<Throwable> onThreadDeath;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile ScheduledExecutorService executor;

    public ProducerRunner(ProducerConfig config, Producer<String, Object> producer, PayloadGenerator generator) {
        this(config, producer, generator, t -> { });
    }

    public ProducerRunner(ProducerConfig config, Producer<String, Object> producer, PayloadGenerator generator,
                          java.util.function.Consumer<Throwable> onThreadDeath) {
        this.config = config;
        this.producer = producer;
        this.generator = generator;
        this.onThreadDeath = onThreadDeath;
    }

    public synchronized void start() {
        if (executor != null) return;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "producer");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(() -> {
            try {
                tick();
            } catch (Throwable t) {
                LOG.error("Producer thread died", t);
                executor = null;
                try {
                    producer.close(Duration.ofSeconds(5));
                } catch (Exception e) {
                    LOG.warnf("Error closing producer after thread death: %s", e.toString());
                }
                onThreadDeath.accept(t);
            }
        }, 0, config.intervalMs(), TimeUnit.MILLISECONDS);
        LOG.infof("Producer started: topic=%s format=%s interval=%dms", config.topic(), config.format(), config.intervalMs());
    }

    public synchronized void stop() {
        ScheduledExecutorService ex = executor;
        if (ex == null) return;
        executor = null;
        ex.shutdown();
        try {
            if (!ex.awaitTermination(5, TimeUnit.SECONDS)) {
                ex.shutdownNow();
                LOG.warnf("Producer scheduler did not terminate within 5s; forced shutdown");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ex.shutdownNow();
        }
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            LOG.warnf("Error closing producer: %s", e.toString());
        }
        LOG.infof("Producer stopped: sent=%d failed=%d", sent.get(), failed.get());
    }

    public boolean isStarted() { return executor != null; }
    public long sent() { return sent.get(); }
    public long failed() { return failed.get(); }

    void tick() {
        Event event = null;
        try {
            event = generator.next();
            Object value = config.format() == Format.STRING ? PayloadGenerator.toJson(event) : event;
            ProducerRecord<String, Object> record = new ProducerRecord<>(config.topic(), event.getId(), value);
            final Event sentEvent = event;
            producer.send(record, (meta, err) -> {
                if (err != null) {
                    failed.incrementAndGet();
                    LOG.warnf("Send failed for seq=%d: %s", sentEvent.getSequence(), err.toString());
                } else {
                    sent.incrementAndGet();
                    LOG.infof("Sent seq=%d key=%s to %s-%d@%d", sentEvent.getSequence(), sentEvent.getId(),
                            meta.topic(), meta.partition(), meta.offset());
                }
            });
        } catch (Exception e) {
            // Covers generator/serializer/record-construction failures as well as synchronous
            // exceptions from send (e.g. registry 403 through the RBAC proxy) -- never let an
            // exception escape tick(), or scheduleAtFixedRate silently cancels the periodic task.
            failed.incrementAndGet();
            LOG.warnf("Send failed for %s: %s",
                    event == null ? "before event creation" : "seq=" + event.getSequence(), e.toString());
        }
    }
}
