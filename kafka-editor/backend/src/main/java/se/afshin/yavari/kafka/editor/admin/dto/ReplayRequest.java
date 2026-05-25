package se.afshin.yavari.kafka.editor.admin.dto;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/messages/replay. The raw record at
 * (sourceTopic, partition, offset) is re-produced to targetTopic, preserving
 * its key/value bytes and headers (so registry envelopes survive intact).
 */
public record ReplayRequest(
        ConnectionConfig connection,
        String sourceTopic,
        Integer partition,
        Long offset,
        String targetTopic) {
}
