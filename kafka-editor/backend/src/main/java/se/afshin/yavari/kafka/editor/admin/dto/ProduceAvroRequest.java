package se.afshin.yavari.kafka.editor.admin.dto;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Body of POST /api/admin/messages/produce-avro. {@code jsonValue} is encoded
 * against {@code schema} (Avro JSON encoding) and prefixed with the Apicurio
 * envelope ({@code 0x00} + the 8-byte {@code globalId}).
 */
public record ProduceAvroRequest(
        ConnectionConfig connection,
        String topic,
        Integer partition,
        String key,
        String jsonValue,
        String schema,
        Long globalId) {
}
