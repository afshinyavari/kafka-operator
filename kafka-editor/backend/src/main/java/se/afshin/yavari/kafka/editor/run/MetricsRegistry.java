package se.afshin.yavari.kafka.editor.run;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-run record counters keyed by editor node id. Each node's interpreted
 * output stream is wrapped with a peek that increments its counter.
 */
public final class MetricsRegistry {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();

    /** Count one record flowing through (out of) the given node. */
    public void increment(String nodeId) {
        counters.computeIfAbsent(nodeId, k -> new AtomicLong()).incrementAndGet();
    }

    /** A snapshot of every node's record count. */
    public Map<String, Long> snapshot() {
        Map<String, Long> out = new HashMap<>();
        counters.forEach((nodeId, count) -> out.put(nodeId, count.get()));
        return out;
    }
}
