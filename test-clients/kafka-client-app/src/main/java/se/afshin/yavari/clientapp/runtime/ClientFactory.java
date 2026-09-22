package se.afshin.yavari.clientapp.runtime;

import se.afshin.yavari.clientapp.config.ConsumerConfig;
import se.afshin.yavari.clientapp.config.Format;
import se.afshin.yavari.clientapp.config.KafkaClientConfig;
import se.afshin.yavari.clientapp.config.ProducerConfig;
import se.afshin.yavari.clientapp.serde.SerdeProps;

import java.util.Properties;

/** Assembles the final kafka-clients {@link Properties} for producer and consumer. */
public final class ClientFactory {

    static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    private ClientFactory() {}

    public static Properties producerProperties(KafkaClientConfig kafka, ProducerConfig pc) {
        Properties p = kafka.toProperties("-producer");
        p.put("key.serializer", STRING_SERIALIZER);
        if (pc.format() == Format.STRING) {
            p.put("value.serializer", STRING_SERIALIZER);
        } else {
            p.putAll(SerdeProps.producer(pc.schema()));
        }
        return p;
    }

    public static Properties consumerProperties(KafkaClientConfig kafka, ConsumerConfig cc) {
        Properties p = kafka.toProperties("-consumer");
        p.put("group.id", cc.groupId());
        p.put("auto.offset.reset", cc.autoOffsetReset());
        p.put("enable.auto.commit", "true");
        p.put("key.deserializer", STRING_DESERIALIZER);
        if (cc.format() == Format.STRING) {
            p.put("value.deserializer", STRING_DESERIALIZER);
        } else {
            p.putAll(SerdeProps.consumer(cc.schema()));
        }
        return p;
    }
}
