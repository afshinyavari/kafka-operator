package se.afshin.yavari.kafka.smt;

/** Result of registering a schema: the id this registry assigned (globalId for Apicurio,
 *  schema id for Confluent) and the version string under the artifact/subject. */
public record Registered(long id, String version) {}
