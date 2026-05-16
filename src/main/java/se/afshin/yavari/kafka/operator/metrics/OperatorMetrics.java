package se.afshin.yavari.kafka.operator.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class OperatorMetrics {

    @Inject MeterRegistry registry;

    public void recordRollingUpdate(String namespace, String pool, boolean success) {
        Counter.builder("kafka_operator_rolling_updates_total")
                .tag("namespace", namespace)
                .tag("pool", pool)
                .tag("result", success ? "success" : "failure")
                .description("Total rolling update attempts per pool")
                .register(registry)
                .increment();
    }

    public void recordIsrCheck(String type, boolean safe) {
        Counter.builder("kafka_operator_isr_checks_total")
                .tag("type", type)
                .tag("result", safe ? "safe" : "unsafe")
                .description("ISR/quorum safety check outcomes")
                .register(registry)
                .increment();
    }

    public void recordScaleDown(String namespace, String pool, boolean safe) {
        Counter.builder("kafka_operator_scale_down_attempts_total")
                .tag("namespace", namespace)
                .tag("pool", pool)
                .tag("result", safe ? "proceeded" : "blocked")
                .description("Scale-down attempts, blocked means ISR not safe yet")
                .register(registry)
                .increment();
    }

    public Timer rollingUpdateTimer(String namespace, String pool) {
        return Timer.builder("kafka_operator_rolling_update_duration_seconds")
                .tag("namespace", namespace)
                .tag("pool", pool)
                .description("Time taken to complete a pod rolling restart")
                .register(registry);
    }
}
