package se.afshin.yavari.clientapp;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.jboss.logging.Logger;
import se.afshin.yavari.clientapp.config.AppConfig;
import se.afshin.yavari.clientapp.config.ConfigException;
import se.afshin.yavari.clientapp.config.Env;
import se.afshin.yavari.clientapp.consumer.ConsumerRunner;
import se.afshin.yavari.clientapp.producer.PayloadGenerator;
import se.afshin.yavari.clientapp.producer.ProducerRunner;
import se.afshin.yavari.clientapp.runtime.ClientFactory;
import se.afshin.yavari.clientapp.runtime.RunnerRegistry;

/**
 * Loads and validates configuration from the environment, builds the Kafka clients,
 * starts the enabled runners and blocks until shutdown.
 */
@QuarkusMain
public class Main implements QuarkusApplication {

    private static final Logger LOG = Logger.getLogger(Main.class);

    /** System property Avro 1.12+ checks before trusting a package's classes for SpecificRecord deserialization. */
    static final String AVRO_SERIALIZABLE_PACKAGES_PROPERTY = "org.apache.avro.SERIALIZABLE_PACKAGES";
    /** Package holding the generated {@code Event} SpecificRecord; must be trusted or Avro throws SecurityException. */
    static final String AVRO_TRUSTED_PACKAGE = "se.afshin.yavari.clientapp.avro";

    @Override
    public int run(String... args) {
        // Avro 1.12 refuses to deserialize into a generated SpecificRecord class unless its
        // package is explicitly trusted (SecurityException: "This class is not trusted to be
        // included in Avro schemas"). Must be set before any config loading or Kafka/Avro class
        // is touched, since the check happens the first time the generated class is used.
        if (System.getProperty(AVRO_SERIALIZABLE_PACKAGES_PROPERTY) == null) {
            System.setProperty(AVRO_SERIALIZABLE_PACKAGES_PROPERTY, AVRO_TRUSTED_PACKAGE);
        }

        AppConfig config;
        try {
            config = AppConfig.load(new Env(System::getenv));
        } catch (ConfigException e) {
            System.err.println("Invalid configuration:");
            e.problems().forEach(p -> System.err.println("  - " + p));
            return 1;
        }

        RunnerRegistry registry = Arc.container().instance(RunnerRegistry.class).get();
        ProducerRunner producer = null;
        ConsumerRunner consumer = null;

        if (config.producer().enabled()) {
            producer = new ProducerRunner(config.producer(),
                    new KafkaProducer<>(ClientFactory.producerProperties(config.kafka(), config.producer())),
                    PayloadGenerator.forThisHost());
            ProducerRunner p = producer;
            registry.register(p::isStarted);
            producer.start();
        }
        if (config.consumer().enabled()) {
            consumer = new ConsumerRunner(config.consumer(),
                    new KafkaConsumer<>(ClientFactory.consumerProperties(config.kafka(), config.consumer())));
            ConsumerRunner c = consumer;
            registry.register(c::isStarted);
            consumer.start();
        }
        LOG.infof("kafka-client-app up: bootstrap=%s protocol=%s producer=%s consumer=%s",
                config.kafka().bootstrapServers(), config.kafka().protocol(),
                config.producer().enabled(), config.consumer().enabled());

        Quarkus.waitForExit();

        if (producer != null) producer.stop();
        if (consumer != null) consumer.stop();
        return 0;
    }
}
