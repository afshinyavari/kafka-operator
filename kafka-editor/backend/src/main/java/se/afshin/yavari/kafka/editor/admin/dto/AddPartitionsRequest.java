package se.afshin.yavari.kafka.editor.admin.dto;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/topics/{name}/partitions. {@code totalCount} is the
 * new total partition count — Kafka only allows increasing it.
 */
public record AddPartitionsRequest(
        ConnectionConfig connection,
        Integer totalCount) {
}
