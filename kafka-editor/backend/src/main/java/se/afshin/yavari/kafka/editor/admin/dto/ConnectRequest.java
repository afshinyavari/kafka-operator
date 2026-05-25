package se.afshin.yavari.kafka.editor.admin.dto;

import com.fasterxml.jackson.databind.JsonNode;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of Kafka Connect write endpoints. {@code config} carries a connector
 * definition for create / config-update; it is null for lifecycle actions.
 */
public record ConnectRequest(ConnectionConfig connection, JsonNode config) {
}
