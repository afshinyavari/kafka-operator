package se.afshin.yavari.rbac;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

@QuarkusTest
class ProxyResourceTest {

    // ── extractArtifact ───────────────────────────────────────────────────────

    @Test
    void extractArtifactFromApicurioPath() {
        assertThat(ProxyResource.extractArtifact("/apis/registry/v2/groups/default/artifacts/orders"))
            .isEqualTo("orders");
        assertThat(ProxyResource.extractArtifact("/apis/registry/v2/groups/default/artifacts/invoices"))
            .isEqualTo("invoices");
    }

    @Test
    void extractArtifactFromApicurioListPath() {
        // Listing artifacts — no specific artifact, return wildcard
        assertThat(ProxyResource.extractArtifact("/apis/registry/v2/groups/default/artifacts"))
            .isEqualTo("*");
    }

    @Test
    void extractArtifactFromApicurioGlobalIdPath() {
        assertThat(ProxyResource.extractArtifact("/apis/registry/v2/ids/globalIds/1"))
            .isEqualTo("*");
    }

    @Test
    void extractArtifactFromXmlSchemaPath() {
        assertThat(ProxyResource.extractArtifact("/schemas/orders")).isEqualTo("orders");
        assertThat(ProxyResource.extractArtifact("/schemas/invoices")).isEqualTo("invoices");
    }

    @Test
    void extractArtifactFromXmlSchemaListPath() {
        assertThat(ProxyResource.extractArtifact("/schemas")).isEqualTo("*");
    }

    // ── resolveAction ─────────────────────────────────────────────────────────

    @Test
    void resolveActionFromHttpMethod() {
        assertThat(ProxyResource.resolveAction("GET",    "/anything")).isEqualTo(READ);
        assertThat(ProxyResource.resolveAction("HEAD",   "/anything")).isEqualTo(READ);
        assertThat(ProxyResource.resolveAction("DELETE", "/anything")).isEqualTo(DELETE);
        assertThat(ProxyResource.resolveAction("POST",   "/anything")).isEqualTo(WRITE);
        assertThat(ProxyResource.resolveAction("PUT",    "/anything")).isEqualTo(WRITE);
    }

    // ── parseIdLookup ─────────────────────────────────────────────────────────

    @Test
    void parseIdLookupGlobalId() {
        ProxyResource.IdLookup l = ProxyResource.parseIdLookup("/apis/registry/v2/ids/globalIds/7");
        assertThat(l).isNotNull();
        assertThat(l.queryParam()).isEqualTo("globalId");
        assertThat(l.id()).isEqualTo("7");
    }

    @Test
    void parseIdLookupContentId() {
        ProxyResource.IdLookup l = ProxyResource.parseIdLookup("/apis/registry/v2/ids/contentIds/42");
        assertThat(l).isNotNull();
        assertThat(l.queryParam()).isEqualTo("contentId");
        assertThat(l.id()).isEqualTo("42");
    }

    @Test
    void parseIdLookupGlobalIdWithReferencesSubpath() {
        ProxyResource.IdLookup l =
            ProxyResource.parseIdLookup("/apis/registry/v2/ids/globalIds/7/references");
        assertThat(l).isNotNull();
        assertThat(l.queryParam()).isEqualTo("globalId");
        assertThat(l.id()).isEqualTo("7");
    }

    @Test
    void parseIdLookupReturnsNullForNonByIdPaths() {
        assertThat(ProxyResource.parseIdLookup(
            "/apis/registry/v2/groups/default/artifacts/orders")).isNull();
        assertThat(ProxyResource.parseIdLookup("/schemas/orders")).isNull();
        // content-hash lookups have no single search param — not resolvable here
        assertThat(ProxyResource.parseIdLookup(
            "/apis/registry/v2/ids/contentHashes/abc123")).isNull();
    }

    // ── parseSearchByIdQuery ──────────────────────────────────────────────────

    @Test
    void parseSearchByIdQueryGlobalId() {
        ProxyResource.IdLookup l = ProxyResource.parseSearchByIdQuery(
            "/apis/registry/v2/search/artifacts", "globalId=11");
        assertThat(l).isNotNull();
        assertThat(l.queryParam()).isEqualTo("globalId");
        assertThat(l.id()).isEqualTo("11");
    }

    @Test
    void parseSearchByIdQueryContentId() {
        ProxyResource.IdLookup l = ProxyResource.parseSearchByIdQuery(
            "/apis/registry/v2/search/artifacts", "limit=1&contentId=5");
        assertThat(l).isNotNull();
        assertThat(l.queryParam()).isEqualTo("contentId");
        assertThat(l.id()).isEqualTo("5");
    }

    @Test
    void parseSearchByIdQueryReturnsNullForGeneralSearch() {
        assertThat(ProxyResource.parseSearchByIdQuery(
            "/apis/registry/v2/search/artifacts", "name=orders")).isNull();
        assertThat(ProxyResource.parseSearchByIdQuery(
            "/apis/registry/v2/search/artifacts", null)).isNull();
        // not a search path
        assertThat(ProxyResource.parseSearchByIdQuery(
            "/apis/registry/v2/groups/default/artifacts/orders", "globalId=1")).isNull();
    }

    // ── firstArtifactId ───────────────────────────────────────────────────────

    @Test
    void firstArtifactIdFromSearchResponse() {
        String json = "{\"artifacts\":[{\"id\":\"mm2-orders-value\",\"name\":\"order\","
            + "\"type\":\"JSON\"}],\"count\":1}";
        assertThat(ProxyResource.firstArtifactId(json)).isEqualTo("mm2-orders-value");
    }

    @Test
    void firstArtifactIdReturnsNullWhenNoArtifacts() {
        assertThat(ProxyResource.firstArtifactId("{\"artifacts\":[],\"count\":0}")).isNull();
    }

    @Test
    void firstArtifactIdReturnsNullForMalformedBody() {
        assertThat(ProxyResource.firstArtifactId("not-json")).isNull();
        assertThat(ProxyResource.firstArtifactId("")).isNull();
    }

    // ── RBAC enforcement via HTTP ─────────────────────────────────────────────

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamIsBlockedFromInvoices() {
        given()
            .get("/apis/registry/v2/groups/default/artifacts/invoices")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "bob", roles = {"invoices-team"})
    void invoicesTeamIsBlockedFromOrders() {
        given()
            .get("/apis/registry/v2/groups/default/artifacts/orders")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamPassesPolicyForOrders() {
        // Upstream not running → 502, but crucially not 403 (policy allowed)
        int status = given()
            .get("/apis/registry/v2/groups/default/artifacts/orders")
            .then()
            .extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "bob", roles = {"invoices-team"})
    void invoicesTeamPassesPolicyForInvoices() {
        int status = given()
            .get("/apis/registry/v2/groups/default/artifacts/invoices")
            .then()
            .extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "schemadmin", roles = {"schema-admin"})
    void schemaAdminPassesPolicyForAnything() {
        int ordersStatus = given()
            .get("/apis/registry/v2/groups/default/artifacts/orders")
            .then().extract().statusCode();
        int invoicesStatus = given()
            .get("/apis/registry/v2/groups/default/artifacts/invoices")
            .then().extract().statusCode();
        assertThat(ordersStatus).isNotEqualTo(403);
        assertThat(invoicesStatus).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamCannotDeleteOrders() {
        // DELETE = DELETE action; orders-team only has READ, WRITE
        given()
            .delete("/apis/registry/v2/groups/default/artifacts/orders")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamBlockedFromXmlSchemaOfInvoices() {
        given()
            .delete("/schemas/invoices")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "schemadmin", roles = {"schema-admin"})
    void schemaAdminCanDeleteXmlSchema() {
        int status = given()
            .delete("/schemas/orders")
            .then().extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    // ── by-id lookups (/ids/globalIds/{id}) ──────────────────────────────────

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void byIdLookupDeniedWhenArtifactUnresolvedAndRoleLacksWildcard() {
        // The test profile points the registry at a dead port, so resolveArtifact()
        // cannot resolve globalId 7 and falls back to "*". orders-team has no "*"
        // schema grant -> 403. (With a live registry it resolves to the real artifact.)
        given()
            .get("/apis/registry/v2/ids/globalIds/7")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "schemadmin", roles = {"schema-admin"})
    void byIdLookupAllowedForWildcardRole() {
        // schema-admin holds artifacts: ["*"], so the by-id lookup passes policy
        // (non-403; 502 because the upstream registry is not running in tests).
        int status = given()
            .get("/apis/registry/v2/ids/globalIds/7")
            .then().extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }
}
