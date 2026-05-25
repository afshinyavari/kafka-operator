package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.Map;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/messages/produce. {@code key}/{@code value} are
 * UTF-8 text; a {@code tombstone} sends a null value. {@code valueType} is
 * "json" (validated) or "string".
 */
public record ProduceRequest(
        ConnectionConfig connection,
        String topic,
        Integer partition,
        String key,
        String value,
        String valueType,
        boolean tombstone,
        Map<String, String> headers) {
}
