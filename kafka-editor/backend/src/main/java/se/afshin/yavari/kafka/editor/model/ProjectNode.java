package se.afshin.yavari.kafka.editor.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A node in the topology. `config` is the heterogeneous property map keyed by
 * PropertySpec.key — read as raw JSON and converted to typed records on demand.
 */
public record ProjectNode(String id, String type, JsonNode config) {
}
