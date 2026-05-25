package se.afshin.yavari.kafka.editor.run;

import java.util.Map;

/** A point-in-time view of a live run, streamed to the front-end over SSE. */
public record MetricsSnapshot(
        String runId,
        String status,
        Map<String, Long> metrics) {
}
