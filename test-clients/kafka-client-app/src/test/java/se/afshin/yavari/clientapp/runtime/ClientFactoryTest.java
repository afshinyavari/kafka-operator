package se.afshin.yavari.clientapp.runtime;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.clientapp.config.*;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.AuthMode;
import se.afshin.yavari.clientapp.config.SchemaRegistryConfig.RegistryType;
import se.afshin.yavari.clientapp.serde.SerdeProps;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ClientFactoryTest {

    private static final TlsStores NONE = new TlsStores(null, null, "PKCS12", null, null, "PKCS12");
    private static final KafkaClientConfig KAFKA =
            new KafkaClientConfig("b:9092", KafkaClientConfig.SecurityProtocol.PLAINTEXT, NONE, null, "app");

    @Test
    void stringProducerUsesStringSerializers() {
        ProducerConfig pc = new ProducerConfig(true, "t", 1000L, Format.STRING, null);
        Properties p = ClientFactory.producerProperties(KAFKA, pc);
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("key.serializer"));
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("value.serializer"));
        assertEquals("app-producer", p.get("client.id"));
        assertEquals("b:9092", p.get("bootstrap.servers"));
    }

    @Test
    void avroProducerMergesSerdeProps() {
        SchemaRegistryConfig s = new SchemaRegistryConfig(RegistryType.APICURIO, "http://r/apis/registry/v3",
                AuthMode.NONE, NONE, null, true, "default");
        ProducerConfig pc = new ProducerConfig(true, "t", 1000L, Format.AVRO, s);
        Properties p = ClientFactory.producerProperties(KAFKA, pc);
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", p.get("key.serializer"));
        assertEquals(SerdeProps.APICURIO_SERIALIZER, p.get("value.serializer"));
        assertEquals("http://r/apis/registry/v3", p.get("apicurio.registry.url"));
    }

    @Test
    void consumerSetsGroupResetAndDeserializers() {
        SchemaRegistryConfig s = new SchemaRegistryConfig(RegistryType.CONFLUENT, "http://sr:8081",
                AuthMode.NONE, NONE, null, true, "default");
        ConsumerConfig cc = new ConsumerConfig(true, "t", "g1", "latest", Format.AVRO, s);
        Properties p = ClientFactory.consumerProperties(KAFKA, cc);
        assertEquals("g1", p.get("group.id"));
        assertEquals("latest", p.get("auto.offset.reset"));
        assertEquals("true", p.get("enable.auto.commit"));
        assertEquals("app-consumer", p.get("client.id"));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", p.get("key.deserializer"));
        assertEquals(SerdeProps.CONFLUENT_DESERIALIZER, p.get("value.deserializer"));
        assertEquals(true, p.get("specific.avro.reader"));
    }

    @Test
    void stringConsumerUsesStringDeserializer() {
        ConsumerConfig cc = new ConsumerConfig(true, "t", "g1", "earliest", Format.STRING, null);
        Properties p = ClientFactory.consumerProperties(KAFKA, cc);
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", p.get("value.deserializer"));
    }
}
