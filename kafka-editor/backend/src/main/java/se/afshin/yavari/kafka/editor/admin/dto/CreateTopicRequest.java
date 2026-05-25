package se.afshin.yavari.kafka.editor.admin.dto;

import java.util.Map;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/** Body of POST /api/admin/topics. {@code configs} is optional. */
public record CreateTopicRequest(
        ConnectionConfig connection,
        String name,
        Integer partitions,
        Integer replicationFactor,
        Map<String, String> configs) {
}
