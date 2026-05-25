package se.afshin.yavari.kafka.editor.admin.dto;

/** One Kafka broker node. {@code rack} may be null. */
public record BrokerInfo(int id, String host, int port, String rack) {
}
