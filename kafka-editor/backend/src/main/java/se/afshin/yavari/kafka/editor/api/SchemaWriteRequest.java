package se.afshin.yavari.kafka.editor.api;

/**
 * Body for creating or updating a schema-registry artifact. {@code type}
 * (AVRO / JSON / PROTOBUF …) is only used on create.
 */
public record SchemaWriteRequest(
        String registry,
        String groupId,
        String artifactId,
        String type,
        String content) {

    public String groupOrDefault() {
        return groupId == null || groupId.isBlank() ? "default" : groupId.trim();
    }
}
