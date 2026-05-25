package se.afshin.yavari.kafka.editor.admin;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Broker-backed lifecycle test: create -> describe -> alter-config ->
 * add-partitions -> metrics -> delete. Skipped by `mvn test`; run with
 * `mvn verify -DskipITs=false` against the docker-compose Kafka.
 */
@QuarkusTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TopicAdminIT {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC =
            "kafka-editor-it-" + System.currentTimeMillis();

    private static String body(String fields) {
        return "{\"connection\":{\"bootstrapServers\":\"" + BOOTSTRAP + "\"},"
                + fields + "}";
    }

    @Test
    @Order(1)
    void createTopic() {
        given().contentType("application/json")
                .body(body("\"name\":\"" + TOPIC
                        + "\",\"partitions\":2,\"replicationFactor\":1"))
                .when().post("/api/admin/topics")
                .then().statusCode(200);
    }

    @Test
    @Order(2)
    void describeShowsTwoPartitions() {
        given().when()
                .get("/api/admin/topics/" + TOPIC + "?bootstrap=" + BOOTSTRAP)
                .then().statusCode(200)
                .body("data.partitions.size()", is(2));
    }

    @Test
    @Order(3)
    void alterConfig() {
        given().contentType("application/json")
                .body(body("\"changes\":{\"retention.ms\":\"60000\"}"))
                .when().post("/api/admin/topics/" + TOPIC + "/config")
                .then().statusCode(200);
    }

    @Test
    @Order(4)
    void addPartitions() {
        given().contentType("application/json")
                .body(body("\"totalCount\":4"))
                .when().post("/api/admin/topics/" + TOPIC + "/partitions")
                .then().statusCode(200);
    }

    @Test
    @Order(5)
    void metricsReportFourPartitions() {
        given().when()
                .get("/api/admin/topics/" + TOPIC + "/metrics?bootstrap="
                        + BOOTSTRAP)
                .then().statusCode(200)
                .body("data.partitionCount", is(4));
    }

    @Test
    @Order(6)
    void deleteTopic() {
        given().when()
                .delete("/api/admin/topics/" + TOPIC + "?bootstrap=" + BOOTSTRAP)
                .then().statusCode(200);
    }
}
