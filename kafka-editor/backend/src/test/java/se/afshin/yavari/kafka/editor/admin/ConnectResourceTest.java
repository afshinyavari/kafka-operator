package se.afshin.yavari.kafka.editor.admin;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

import org.junit.jupiter.api.Test;

/** With no Connect URL, the Connect endpoints report a capability, not an error. */
@QuarkusTest
@TestSecurity(user = "test")
class ConnectResourceTest {

    @Test
    void connectorsWithoutAUrlReportNotConfigured() {
        given().when()
                .get("/api/admin/connect/connectors")
                .then().statusCode(200)
                .body("capability", is("not_configured"));
    }
}
