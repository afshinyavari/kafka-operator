package se.afshin.yavari.kafka.editor.run;

import java.util.Map;

/**
 * The outcome of a run: per-node record counts keyed by editor node id, plus an
 * optional error message when the topology could not be built or driven.
 */
public record RunResult(
        String runId,
        String mode,
        Map<String, Long> metrics,
        String error) {
}
