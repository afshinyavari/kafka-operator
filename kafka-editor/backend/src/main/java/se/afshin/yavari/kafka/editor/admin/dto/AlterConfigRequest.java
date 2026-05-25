package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.Map;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/topics/{name}/config. Each entry in {@code changes}
 * is a config key to alter; a blank value resets the key to its default.
 */
public record AlterConfigRequest(
        ConnectionConfig connection,
        Map<String, String> changes) {
}
