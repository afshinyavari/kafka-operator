package se.afshin.yavari.clientapp.runtime;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.config.AppConfig;
import se.afshin.yavari.clientapp.consumer.ConsumerRunner;
import se.afshin.yavari.clientapp.producer.PayloadGenerator;
import se.afshin.yavari.clientapp.producer.ProducerRunner;

/**
 * Builds and starts the enabled producer/consumer runners, rolling back anything already
 * started if a later step fails, and stops them all together on shutdown.
 */
public final class Runners {

    private static final Logger LOG = Logger.getLogger(Runners.class);

    private ProducerRunner producer;
    private ConsumerRunner consumer;

    private Runners() {}

    /**
     * Builds and starts the producer (if enabled) then the consumer (if enabled), registering
     * each runner's {@code isStarted} with {@code registry} before starting it. If anything
     * throws after a runner was started, every runner started so far is stopped (each in its
     * own try/catch, so a second failure cannot mask the first) before the original exception
     * is rethrown.
     */
    public static Runners start(AppConfig config, RunnerRegistry registry) {
        Runners runners = new Runners();
        try {
            if (config.producer().enabled()) {
                runners.producer = new ProducerRunner(config.producer(),
                        new KafkaProducer<>(ClientFactory.producerProperties(config.kafka(), config.producer())),
                        PayloadGenerator.forThisHost());
                ProducerRunner p = runners.producer;
                registry.register(p::isStarted);
                runners.producer.start();
            }
            if (config.consumer().enabled()) {
                runners.consumer = new ConsumerRunner(config.consumer(),
                        new KafkaConsumer<>(ClientFactory.consumerProperties(config.kafka(), config.consumer())));
                ConsumerRunner c = runners.consumer;
                registry.register(c::isStarted);
                runners.consumer.start();
            }
            return runners;
        } catch (RuntimeException | Error e) {
            runners.stopAll();
            throw e;
        }
    }

    /** Stops consumer then producer; each guarded so a failure in one does not prevent the other. Never throws. */
    public void stopAll() {
        if (consumer != null) {
            try {
                consumer.stop();
            } catch (Exception e) {
                LOG.warnf("Error stopping consumer: %s", e.toString());
            }
        }
        if (producer != null) {
            try {
                producer.stop();
            } catch (Exception e) {
                LOG.warnf("Error stopping producer: %s", e.toString());
            }
        }
    }

    ProducerRunner producer() { return producer; }
    ConsumerRunner consumer() { return consumer; }
}
