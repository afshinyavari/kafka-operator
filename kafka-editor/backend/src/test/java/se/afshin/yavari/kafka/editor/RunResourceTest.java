package se.afshin.yavari.kafka.editor;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

/** End-to-end test of POST /api/run in test mode. */
@QuarkusTest
@TestSecurity(user = "test")
class RunResourceTest {

    /** A Source -> Filter (value >= 5) -> Sink topology. */
    private static final String TOPOLOGY = """
        {
          "schemaVersion": 3,
          "id": "test",
          "meta": { "name": "Test" },
          "nodes": [
            { "id": "src", "type": "source", "config": { "topicId": "t1" } },
            { "id": "flt", "type": "filter", "config": { "predicate": {
                "combinator": "AND",
                "conditions": [
                  { "id": "c1", "field": "value", "operator": "gte", "value": "5" }
                ] } } },
            { "id": "snk", "type": "sink", "config": { "topicId": "t2" } }
          ],
          "edges": [
            { "id": "e1", "source": "src", "sourceHandle": "out",
              "target": "flt", "targetHandle": "in" },
            { "id": "e2", "source": "flt", "sourceHandle": "out",
              "target": "snk", "targetHandle": "in" }
          ],
          "recordTypes": [],
          "catalog": {
            "topics": [
              { "id": "t1", "name": "input" },
              { "id": "t2", "name": "output" }
            ],
            "serdes": []
          }
        }
        """;

    @Test
    void runsAStatelessTopologyAndReportsPerNodeCounts() {
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
                // 20 records in; the filter keeps value >= 5 (indices 5..19 = 15).
                .body("metrics.src", equalTo(20))
                .body("metrics.flt", equalTo(15))
                .body("metrics.snk", equalTo(15));
    }
}
