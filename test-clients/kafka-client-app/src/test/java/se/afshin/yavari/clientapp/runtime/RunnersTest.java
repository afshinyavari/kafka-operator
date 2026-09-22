package se.afshin.yavari.clientapp.runtime;

import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.AppConfig;
import se.afshin.yavari.clientapp.config.ConsumerConfig;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.KafkaClientConfig;
import se.afshin.yavari.clientapp.config.KafkaClientConfig.SecurityProtocol;
import se.afshin.yavari.clientapp.config.ProducerConfig;
import se.afshin.yavari.clientapp.config.TlsStores;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunnersTest {

    private static final TlsStores NONE = new TlsStores(null, null, "PKCS12", null, null, "PKCS12");
    private static final KafkaClientConfig PLAINTEXT =
            new KafkaClientConfig("localhost:1", SecurityProtocol.PLAINTEXT, NONE, null, "app");

    @Test
    void producerStoppedWhenConsumerConstructionFails() throws InterruptedException {
        ProducerConfig producer = new ProducerConfig(true, "t", 1000L, Format.STRING, null);
        ConsumerConfig consumer = new ConsumerConfig(true, "t", "g1", "bogus", Format.STRING, null);
        AppConfig config = new AppConfig(PLAINTEXT, producer, consumer);
        RunnerRegistry registry = new RunnerRegistry();

        Exception ex = assertThrows(Exception.class, () -> Runners.start(config, registry));
        assertInstanceOf(KafkaException.class, ex);
        assertFalse(registry.allStarted());

        // The producer that was started before the consumer construction failed must have been
        // stopped by the rollback; give its network thread a bounded window to actually exit.
        Instant deadline = Instant.now().plus(Duration.ofSeconds(3));
        boolean stillAlive;
        do {
            stillAlive = Thread.getAllStackTraces().keySet().stream()
                    .anyMatch(t -> t.getName().contains("kafka-producer-network-thread") && t.isAlive());
            if (!stillAlive) break;
            Thread.sleep(50);
        } while (Instant.now().isBefore(deadline));
        assertFalse(stillAlive, "producer network thread should have been stopped by rollback");
    }

    @Test
    void badKeystoreFailsFastWithMessage() {
        TlsStores badKeystore = new TlsStores("/nonexistent/keystore.p12", "x", "PKCS12",
                "/nonexistent/keystore.p12", "x", "PKCS12");
        KafkaClientConfig ssl = new KafkaClientConfig("localhost:1", SecurityProtocol.SSL, badKeystore, null, "app");
        ProducerConfig producer = new ProducerConfig(true, "t", 1000L, Format.STRING, null);
        ConsumerConfig consumer = new ConsumerConfig(false, null, "g1", "earliest", Format.STRING, null);
        AppConfig config = new AppConfig(ssl, producer, consumer);
        RunnerRegistry registry = new RunnerRegistry();

        Exception ex = assertThrows(Exception.class, () -> Runners.start(config, registry));
        assertInstanceOf(KafkaException.class, ex);
        boolean hasMessage = ex.getMessage() != null || ex.getCause() != null;
        assertTrue(hasMessage, "expected a readable message or cause, not a bare exception");
    }

    @Test
    void happyPathStartsAndStopsBoth() {
        ProducerConfig producer = new ProducerConfig(true, "t", 1000L, Format.STRING, null);
        ConsumerConfig consumer = new ConsumerConfig(true, "t", "g1", "earliest", Format.STRING, null);
        AppConfig config = new AppConfig(PLAINTEXT, producer, consumer);
        RunnerRegistry registry = new RunnerRegistry();
        java.util.concurrent.atomic.AtomicReference<Throwable> deathCallback = new java.util.concurrent.atomic.AtomicReference<>();

        Runners runners = Runners.start(config, registry, deathCallback::set);
        assertNotNull(runners);
        assertTrue(registry.allStarted());

        assertDoesNotThrow(runners::stopAll);

        assertFalse(runners.producer().isStarted());
        assertFalse(runners.consumer().isStarted());
    }
}
