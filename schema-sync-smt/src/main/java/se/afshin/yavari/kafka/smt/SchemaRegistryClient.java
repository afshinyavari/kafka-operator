package se.afshin.yavari.kafka.smt;

/**
 * Registry-neutral operations the SMT needs. Implemented by {@link ApicurioClient}
 * (Apicurio REST v2) and {@link ConfluentClient} (Confluent Schema Registry REST, which
 * Apicurio also serves under {@code /apis/ccompat/v7}).
 */
public interface SchemaRegistryClient extends AutoCloseable {

    /** Fetches the schema carried in a record envelope by its id. */
    RegistrySchema fetchById(long id) throws RegistryException;

    /** Creates the schema, or returns the existing identical one. Idempotent. */
    Registered upsert(RegistrySchema schema) throws RegistryException;

    /** Resolves a reference's coordinates to an id in this registry, or {@code null}
     *  if this registry cannot resolve it (the caller then relies on server-side
     *  resolution at upsert time). A {@code null} version means "latest". */
    Long lookupId(SchemaRef ref) throws RegistryException;

    @Override
    void close();
}
