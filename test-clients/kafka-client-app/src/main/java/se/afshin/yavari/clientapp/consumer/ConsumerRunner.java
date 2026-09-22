package se.afshin.yavari.clientapp.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.config.ConsumerConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Polls a topic on a dedicated thread and logs every record. Errors are logged and retried. */
public final class ConsumerRunner {

    private static final Logger LOG = Logger.getLogger(ConsumerRunner.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final ConsumerConfig config;
    private final Consumer<String, Object> consumer;
    private final java.util.function.Consumer<Throwable> onThreadDeath;
    private final AtomicLong received = new AtomicLong();
    private volatile boolean running;
    private Thread thread;

    public ConsumerRunner(ConsumerConfig config, Consumer<String, Object> consumer) {
        this(config, consumer, t -> { });
    }

    public ConsumerRunner(ConsumerConfig config, Consumer<String, Object> consumer,
                          java.util.function.Consumer<Throwable> onThreadDeath) {
        this.config = config;
        this.consumer = consumer;
        this.onThreadDeath = onThreadDeath;
    }

    public synchronized void start() {
        if (running) return;
        Thread t = new Thread(this::loop, "consumer");
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((th, e) -> {
            running = false;
            LOG.errorf(e, "Consumer thread died");
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception ex) {
                LOG.warnf("Error closing consumer after thread death: %s", ex.toString());
            }
            onThreadDeath.accept(e);
        });
        consumer.subscribe(List.of(config.topic()));
        running = true;
        thread = t;
        thread.start();
        LOG.infof("Consumer started: topic=%s group=%s format=%s", config.topic(), config.groupId(), config.format());
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        consumer.wakeup();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            LOG.warnf("Consumer thread did not stop within 5s; closing anyway");
        }
        try {
            consumer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            LOG.warnf("Error closing consumer: %s", e.toString());
        }
        LOG.infof("Consumer stopped: received=%d", received.get());
    }

    public boolean isStarted() { return running; }
    public long received() { return received.get(); }

    private void loop() {
        while (running) {
            pollOnce();
        }
    }

    /** One poll; returns the number of records handled. Never throws except on wakeup during shutdown. */
    int pollOnce() {
        try {
            ConsumerRecords<String, Object> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, Object> r : records) {
                received.incrementAndGet();
                LOG.infof("Received key=%s %s-%d@%d value=%s", r.key(), r.topic(), r.partition(), r.offset(), r.value());
            }
            return records.count();
        } catch (WakeupException e) {
            if (running) LOG.debug("Wakeup while running; ignoring");
            return 0;
        } catch (Exception e) {
            LOG.warnf("Poll failed: %s", e.toString());
            return 0;
        }
    }
}
