package se.afshin.yavari.kafka.operator.crd;

/** Schema registry wire format. v1 supports APICURIO (Apicurio V3 envelope: magic byte +
 *  8-byte globalId). CONFLUENT is reserved for a future release — the wire format differs
 *  (4-byte schema ID, separate parse path) and the operator validates against it. */
public enum SchemaRegistryType {
    APICURIO,
    CONFLUENT
}
