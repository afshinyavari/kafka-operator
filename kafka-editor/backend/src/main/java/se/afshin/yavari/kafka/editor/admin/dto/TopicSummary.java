package se.afshin.yavari.kafka.editor.admin.dto;

/** One row in the topic list. */
public record TopicSummary(
        String name,
        int partitionCount,
        int replicationFactor,
        boolean internal) {
}
