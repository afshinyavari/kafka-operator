package se.afshin.yavari.clientapp.runtime;

import io.quarkus.runtime.Quarkus;
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

    /** Exit code used when a runner's dedicated thread dies from an escaped {@link Error}. */
    public static final int EXIT_THREAD_DEATH = 3;

    private ProducerRunner producer;
    private ConsumerRunner consumer;

    private Runners() {}

    /** Delegates to the 3-arg overload with an action that exits the process. */
    public static Runners start(AppConfig config, RunnerRegistry registry) {
        return start(config, registry, Runners::exitOnThreadDeath);
    }

    /**
     * Builds and starts the producer (if enabled) then the consumer (if enabled), registering
     * each runner's {@code isStarted} with {@code registry} before starting it. If anything
     * throws after a runner was started, every runner started so far is stopped (each in its
     * own try/catch, so a second failure cannot mask the first) before the original exception
     * is rethrown. {@code onThreadDeath} is invoked if either runner's dedicated thread dies
     * from an escaped {@link Error}.
     */
    public static Runners start(AppConfig config, RunnerRegistry registry,
                                java.util.function.Consumer<Throwable> onThreadDeath) {
        Runners runners = new Runners();
        try {
            if (config.producer().enabled()) {
                runners.producer = new ProducerRunner(config.producer(),
                        new KafkaProducer<>(ClientFactory.producerProperties(config.kafka(), config.producer())),
                        PayloadGenerator.forThisHost(), onThreadDeath);
                ProducerRunner p = runners.producer;
                registry.register(p::isStarted);
                runners.producer.start();
            }
            if (config.consumer().enabled()) {
                runners.consumer = new ConsumerRunner(config.consumer(),
                        new KafkaConsumer<>(ClientFactory.consumerProperties(config.kafka(), config.consumer())),
                        onThreadDeath);
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

    private static void exitOnThreadDeath(Throwable t) {
        Quarkus.asyncExit(EXIT_THREAD_DEATH);
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
