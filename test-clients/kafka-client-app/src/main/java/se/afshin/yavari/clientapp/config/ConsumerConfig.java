package se.afshin.yavari.clientapp.config;

/** {@code CONSUMER_*} environment. {@code schema} is non-null only for {@link Format#AVRO}. */
public record ConsumerConfig(boolean enabled, String topic, String groupId, String autoOffsetReset,
                             Format format, SchemaRegistryConfig schema) {

    public static final String SCHEMA_PREFIX = "CONSUMER_SCHEMA_";
    public static final String DEFAULT_GROUP_ID = "kafka-client-app";
    public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

    public static ConsumerConfig from(Env env, Problems problems) {
        boolean enabled = env.getBoolean("CONSUMER_ENABLED", false);
        if (!enabled) {
            return new ConsumerConfig(false, null, DEFAULT_GROUP_ID, DEFAULT_AUTO_OFFSET_RESET, Format.STRING, null);
        }
        String topic = env.require("CONSUMER_TOPIC", problems);
        String groupId = env.get("CONSUMER_GROUP_ID", DEFAULT_GROUP_ID);
        String reset = env.get("CONSUMER_AUTO_OFFSET_RESET", DEFAULT_AUTO_OFFSET_RESET);
        Format format = env.getEnum("CONSUMER_FORMAT", Format.class, Format.STRING, problems);
        SchemaRegistryConfig schema = format == Format.AVRO
                ? SchemaRegistryConfig.from(env, SCHEMA_PREFIX, problems) : null;
        return new ConsumerConfig(true, topic, groupId, reset, format, schema);
    }
}
