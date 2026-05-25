package se.afshin.yavari.kafka.editor;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

/** End-to-end test of a stateful topology (group-by-key → count). */
@QuarkusTest
@TestSecurity(user = "test")
class StatefulRunTest {

    private static final String TOPOLOGY = """
        {
          "schemaVersion": 3,
          "id": "stateful",
          "meta": { "name": "Stateful" },
          "nodes": [
            { "id": "src", "type": "source", "config": {} },
            { "id": "grp", "type": "group-by-key", "config": {} },
            { "id": "cnt", "type": "count", "config": {} },
            { "id": "snk", "type": "sink", "config": {} }
          ],
          "edges": [
            { "id": "e1", "source": "src", "sourceHandle": "out",
              "target": "grp", "targetHandle": "in" },
            { "id": "e2", "source": "grp", "sourceHandle": "out",
              "target": "cnt", "targetHandle": "in" },
            { "id": "e3", "source": "cnt", "sourceHandle": "out",
              "target": "snk", "targetHandle": "in" }
          ],
          "recordTypes": [],
          "catalog": { "topics": [], "serdes": [] }
        }
        """;

    @Test
    void runsAStatefulGroupByCountTopology() {
        String body = "{ \"document\": " + TOPOLOGY
                + ", \"mode\": \"test\", \"recordsPerSource\": 20 }";
        given()
                .contentType("application/json")
                .body(body)
                .when()
                .post("/api/run")
                .then()
                .statusCode(200)
                .body("error", nullValue())
                .body("metrics.src", equalTo(20))
                .body("metrics.grp", equalTo(20))
                // The count operator built a real state store and emitted updates.
                .body("metrics.cnt", greaterThan(0))
                .body("metrics.snk", greaterThan(0));
    }
}
