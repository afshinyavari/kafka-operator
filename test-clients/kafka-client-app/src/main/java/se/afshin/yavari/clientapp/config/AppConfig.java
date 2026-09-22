package se.afshin.yavari.clientapp.config;

/** Whole configuration. {@link #load} reports every problem at once via {@link ConfigException}. */
public record AppConfig(KafkaClientConfig kafka, ProducerConfig producer, ConsumerConfig consumer) {

    public static AppConfig load(Env env) {
        Problems problems = new Problems();
        KafkaClientConfig kafka = KafkaClientConfig.from(env, problems);
        ProducerConfig producer = ProducerConfig.from(env, problems);
        ConsumerConfig consumer = ConsumerConfig.from(env, problems);
        if (!producer.enabled() && !consumer.enabled()) {
            problems.add("at least one of PRODUCER_ENABLED or CONSUMER_ENABLED must be true");
        }
        if (!problems.isEmpty()) {
            throw new ConfigException(problems);
        }
        return new AppConfig(kafka, producer, consumer);
    }
}
