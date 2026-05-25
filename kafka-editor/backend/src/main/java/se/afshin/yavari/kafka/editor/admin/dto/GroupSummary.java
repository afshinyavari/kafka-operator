package se.afshin.yavari.kafka.editor.admin.dto;

/** One row in the consumer-group list. */
public record GroupSummary(
        String groupId,
        String state,
        int members,
        String assignor,
        String coordinator) {
}
