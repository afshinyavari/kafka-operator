package se.afshin.yavari.clientapp.config;

/** {@code PRODUCER_*} environment. {@code schema} is non-null only for {@link Format#AVRO}. */
public record ProducerConfig(boolean enabled, String topic, long intervalMs, Format format,
                             SchemaRegistryConfig schema) {

    public static final String SCHEMA_PREFIX = "PRODUCER_SCHEMA_";
    public static final long DEFAULT_INTERVAL_MS = 1000L;

    public static ProducerConfig from(Env env, Problems problems) {
        boolean enabled = env.getBoolean("PRODUCER_ENABLED", false);
        if (!enabled) {
            return new ProducerConfig(false, null, DEFAULT_INTERVAL_MS, Format.STRING, null);
        }
        String topic = env.require("PRODUCER_TOPIC", problems);
        long interval = env.getLong("PRODUCER_INTERVAL_MS", DEFAULT_INTERVAL_MS, problems);
        if (interval <= 0) {
            problems.add("PRODUCER_INTERVAL_MS must be > 0, got " + interval);
        }
        Format format = env.getEnum("PRODUCER_FORMAT", Format.class, Format.STRING, problems);
        SchemaRegistryConfig schema = format == Format.AVRO
                ? SchemaRegistryConfig.from(env, SCHEMA_PREFIX, problems) : null;
        return new ProducerConfig(true, topic, interval, format, schema);
    }
}
