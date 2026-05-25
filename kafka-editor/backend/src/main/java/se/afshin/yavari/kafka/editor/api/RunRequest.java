package se.afshin.yavari.kafka.editor.api;

import java.util.Map;

import se.afshin.yavari.kafka.editor.model.ProjectDocument;

/**
 * A request to run a topology.
 *
 * <ul>
 *   <li>{@code mode} — "test" (TopologyTestDriver) or "live" (real broker).</li>
 *   <li>{@code recordsPerSource} — sample records per source, test mode.</li>
 *   <li>{@code connection} — broker + schema registry, live mode.</li>
 *   <li>{@code generateInput} — feed generated records into the source topics.</li>
 *   <li>{@code envVars} — extra Kafka client config (local-only secrets).</li>
 * </ul>
 */
public record RunRequest(
        ProjectDocument document,
        String mode,
        Integer recordsPerSource,
        ConnectionConfig connection,
        Boolean generateInput,
        Map<String, String> envVars) {
}
