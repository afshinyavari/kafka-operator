package se.afshin.yavari.kafka.editor.admin;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Broker-backed message workbench test: produce -> browse -> seek -> tombstone
 * -> replay -> search. Run with `mvn verify -DskipITs=false`.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MessageBrowseIT {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final long STAMP = System.currentTimeMillis();
    private static final String TOPIC = "kafka-editor-msg-it-" + STAMP;
    private static final String REPLAY_TOPIC = "kafka-editor-replay-it-" + STAMP;

    private static String connBody(String fields) {
        return "{\"connection\":{\"bootstrapServers\":\"" + BOOTSTRAP + "\"},"
                + fields + "}";
    }

    private static void produce(String topic, String key, String value) {
        given().contentType("application/json")
                .body(connBody("\"topic\":\"" + topic + "\",\"key\":\"" + key
                        + "\",\"value\":\"" + value
                        + "\",\"valueType\":\"string\",\"tombstone\":false"))
                .when().post("/api/admin/messages/produce")
                .then().statusCode(200);
    }

    @Test
    @Order(1)
    void createTopics() {
        for (String topic : new String[] {TOPIC, REPLAY_TOPIC}) {
            given().contentType("application/json")
                    .body(connBody("\"name\":\"" + topic
                            + "\",\"partitions\":1,\"replicationFactor\":1"))
                    .when().post("/api/admin/topics")
                    .then().statusCode(200);
        }
    }

    @Test
    @Order(2)
    void produceRecords() {
        produce(TOPIC, "k0", "hello-world-0");
        produce(TOPIC, "k1", "hello-world-1");
        produce(TOPIC, "k2", "hello-world-2");
    }

    @Test
    @Order(3)
    void browseReturnsAllRecords() {
        given().when()
                .get("/api/admin/messages?bootstrap=" + BOOTSTRAP + "&topic="
                        + TOPIC + "&seek=LATEST&size=10")
                .then().statusCode(200)
                .body("data.rows.size()", is(3))
                .body("data.rows[0].value.strategy", is("STRING"));
    }

    @Test
    @Order(4)
    void browseFromAnExplicitOffset() {
        given().when()
                .get("/api/admin/messages?bootstrap=" + BOOTSTRAP + "&topic="
                        + TOPIC + "&seek=OFFSET&offset=1&size=10")
                .then().statusCode(200)
                .body("data.rows[0].offset", is(1));
    }

    @Test
    @Order(5)
    void tombstonesAreRendered() {
        given().contentType("application/json")
                .body(connBody("\"topic\":\"" + TOPIC
                        + "\",\"key\":\"k0\",\"tombstone\":true"))
                .when().post("/api/admin/messages/produce")
                .then().statusCode(200);
        given().when()
                .get("/api/admin/messages?bootstrap=" + BOOTSTRAP + "&topic="
                        + TOPIC + "&seek=LATEST&size=10")
                .then().statusCode(200)
                .body("data.rows.value.strategy", hasItem("TOMBSTONE"));
    }

    @Test
    @Order(6)
    void replayCopiesARecord() {
        given().contentType("application/json")
                .body(connBody("\"sourceTopic\":\"" + TOPIC
                        + "\",\"partition\":0,\"offset\":0,\"targetTopic\":\""
                        + REPLAY_TOPIC + "\""))
                .when().post("/api/admin/messages/replay")
                .then().statusCode(200)
                .body("data.topic", is(REPLAY_TOPIC));
        given().when()
                .get("/api/admin/messages?bootstrap=" + BOOTSTRAP + "&topic="
                        + REPLAY_TOPIC + "&seek=LATEST&size=10")
                .then().statusCode(200)
                .body("data.rows.size()", is(1));
    }

    @Test
    @Order(7)
    void searchFindsMatchingRecords() {
        given().when()
                .get("/api/admin/messages/search?bootstrap=" + BOOTSTRAP
                        + "&topic=" + TOPIC
                        + "&partition=0&query=hello-world&scanLimit=1000")
                .then().statusCode(200)
                .body("data.matches.size()", greaterThanOrEqualTo(3));
    }

    @Test
    @Order(8)
    void cleanUp() {
        for (String topic : new String[] {TOPIC, REPLAY_TOPIC}) {
            given().when()
                    .delete("/api/admin/topics/" + topic + "?bootstrap="
                            + BOOTSTRAP)
                    .then().statusCode(200);
        }
    }
}
