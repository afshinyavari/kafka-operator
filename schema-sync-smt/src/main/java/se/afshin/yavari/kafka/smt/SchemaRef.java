package se.afshin.yavari.kafka.smt;

/**
 * A reference from one schema to another, in registry-neutral coordinates.
 *
 * <p>Apicurio: {@code groupId}/{@code artifactId}/{@code version}. Confluent: the subject
 * maps to {@code artifactId} with {@code groupId = "default"}; {@code version} is the
 * subject version number as a string. {@code name} is the import name used inside the
 * referencing schema (e.g. a Protobuf file name or Avro type name).
 */
public record SchemaRef(String name, String groupId, String artifactId, String version) {

    public SchemaRef withVersion(String newVersion) {
        return new SchemaRef(name, groupId, artifactId, newVersion);
    }
}
