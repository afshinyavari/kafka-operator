package se.afshin.yavari.kafka.editor.admin;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;

import org.junit.jupiter.api.Test;

/**
 * Broker-backed ACL test. The docker-compose broker runs without an authorizer,
 * so the ACL endpoint must report a capability — never a 500.
 */
@QuarkusTest
class AclIT {

    @Test
    void aclsNeverFailHard() {
        given().when()
                .get("/api/admin/acls?bootstrap=localhost:9092")
                .then().statusCode(200)
                .body("capability",
                        anyOf(is("unsupported"), is("supported")));
    }
}
