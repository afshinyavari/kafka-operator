package se.afshin.yavari.kafka.editor.admin.dto;

/**
 * A native Kafka ACL. {@code resourceType} (TOPIC/GROUP/CLUSTER/…),
 * {@code patternType} (LITERAL/PREFIXED), {@code operation} (READ/WRITE/…)
 * and {@code permissionType} (ALLOW/DENY) are Kafka enum names.
 */
public record AclEntry(
        String resourceType,
        String resourceName,
        String patternType,
        String principal,
        String host,
        String operation,
        String permissionType) {
}
