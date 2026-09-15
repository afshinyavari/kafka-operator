package se.afshin.yavari.kafka.smt;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A schema as fetched from, or to be registered in, a registry. Registry-neutral:
 * Confluent subjects appear as {@code artifactId} in group {@code "default"}, and
 * {@code type} uses the shared vocabulary {@code AVRO | PROTOBUF | JSON}.
 */
public record RegistrySchema(String groupId, String artifactId, String type, byte[] content,
                             List<SchemaRef> references) {

    public RegistrySchema withReferences(List<SchemaRef> newReferences) {
        return new RegistrySchema(groupId, artifactId, type, content, newReferences);
    }

    public RegistrySchema withArtifactId(String newArtifactId) {
        return new RegistrySchema(groupId, newArtifactId, type, content, references);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RegistrySchema other)) return false;
        return Objects.equals(groupId, other.groupId)
                && Objects.equals(artifactId, other.artifactId)
                && Objects.equals(type, other.type)
                && Arrays.equals(content, other.content)
                && Objects.equals(references, other.references);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, artifactId, type, Arrays.hashCode(content), references);
    }

    @Override
    public String toString() {
        return "RegistrySchema[" + groupId + "/" + artifactId + " type=" + type
                + " bytes=" + (content == null ? 0 : content.length) + " refs=" + references + "]";
    }
}
