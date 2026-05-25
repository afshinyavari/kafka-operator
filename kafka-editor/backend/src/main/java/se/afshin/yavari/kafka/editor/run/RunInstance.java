package se.afshin.yavari.kafka.editor.run;

import java.time.Duration;

import org.apache.kafka.streams.KafkaStreams;

/** A live run — a running KafkaStreams instance plus its metrics and producer. */
public final class RunInstance {

    private final String runId;
    private final KafkaStreams streams;
    private final MetricsRegistry metrics;
    private final TestDataProducer producer;

    public RunInstance(
            String runId,
            KafkaStreams streams,
            MetricsRegistry metrics,
            TestDataProducer producer) {
        this.runId = runId;
        this.streams = streams;
        this.metrics = metrics;
        this.producer = producer;
    }

    public String runId() {
        return runId;
    }

    public MetricsRegistry metrics() {
        return metrics;
    }

    public String status() {
        KafkaStreams.State state = streams.state();
        if (state == KafkaStreams.State.ERROR) {
            return "error";
        }
        if (state == KafkaStreams.State.NOT_RUNNING
                || state == KafkaStreams.State.PENDING_SHUTDOWN) {
            return "stopped";
        }
        return "running";
    }

    /** Stop the producer and the streams instance. */
    public void close() {
        if (producer != null) {
            producer.close();
        }
        try {
            streams.close(Duration.ofSeconds(5));
        } catch (Exception ignored) {
            // best effort
        }
    }
}
