package se.afshin.yavari.kafka.editor.admin;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Broker-backed consumer-group test: seed a group with a committed offset,
 * then list it, read its lag, reset and delete it.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class GroupIT {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final long STAMP = System.currentTimeMillis();
    private static final String TOPIC = "kafka-editor-grp-it-" + STAMP;
    private static final String GROUP = "kafka-editor-grp-" + STAMP;

    private static String connBody(String fields) {
        return "{\"connection\":{\"bootstrapServers\":\"" + BOOTSTRAP + "\"},"
                + fields + "}";
    }

    @Test
    @Order(1)
    void createTopicAndProduce() {
        given().contentType("application/json")
                .body(connBody("\"name\":\"" + TOPIC
                        + "\",\"partitions\":1,\"replicationFactor\":1"))
                .when().post("/api/admin/topics")
                .then().statusCode(200);
        for (int i = 0; i < 5; i++) {
            given().contentType("application/json")
                    .body(connBody("\"topic\":\"" + TOPIC + "\",\"key\":\"k" + i
                            + "\",\"value\":\"v" + i
                            + "\",\"valueType\":\"string\",\"tombstone\":false"))
                    .when().post("/api/admin/messages/produce")
                    .then().statusCode(200);
        }
    }

    @Test
    @Order(2)
    void seedConsumerGroupWithACommittedOffset() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        TopicPartition tp = new TopicPartition(TOPIC, 0);
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(2)));
        }
    }

    @Test
    @Order(3)
    void groupIsListed() {
        given().when()
                .get("/api/admin/groups?bootstrap=" + BOOTSTRAP)
                .then().statusCode(200)
                .body("data.groupId", hasItem(GROUP));
    }

    @Test
    @Order(4)
    void offsetsReportLag() {
        // committed offset 2, end offset 5 -> total lag 3
        given().when()
                .get("/api/admin/groups/" + GROUP + "/offsets?bootstrap="
                        + BOOTSTRAP)
                .then().statusCode(200)
                .body("data.totalLag", is(3));
    }

    @Test
    @Order(5)
    void offsetsCanBeReset() {
        given().contentType("application/json")
                .body(connBody("\"topic\":\"" + TOPIC
                        + "\",\"target\":\"EARLIEST\""))
                .when().post("/api/admin/groups/" + GROUP + "/reset-offsets")
                .then().statusCode(200);
    }

    @Test
    @Order(6)
    void groupCanBeDeleted() {
        given().when()
                .delete("/api/admin/groups/" + GROUP + "?bootstrap=" + BOOTSTRAP)
                .then().statusCode(200);
    }

    @Test
    @Order(7)
    void cleanUp() {
        given().when()
                .delete("/api/admin/topics/" + TOPIC + "?bootstrap=" + BOOTSTRAP)
                .then().statusCode(200);
    }
}
