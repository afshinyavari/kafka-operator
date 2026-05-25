package se.afshin.yavari.kafka.editor.admin.dto;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/groups/{id}/reset-offsets. {@code partition} null
 * resets every partition of the topic. {@code target} is EARLIEST, LATEST,
 * OFFSET (uses {@code offset}) or TIMESTAMP (uses {@code timestamp}).
 */
public record ResetOffsetsRequest(
        ConnectionConfig connection,
        String topic,
        Integer partition,
        String target,
        Long offset,
        Long timestamp) {
}
