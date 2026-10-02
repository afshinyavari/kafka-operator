package se.afshin.yavari.rbac;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.SecurityAttribute;
import io.quarkus.test.security.TestSecurity;
import jakarta.inject.Inject;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

@QuarkusTest
class ProxyResourceTest {

    // ── upstream routing ──────────────────────────────────────────────────────

    @Test
    void everyApicurioApiIsRoutedToTheRegistry() {
        assertThat(ProxyResource.upstreamBase("/apis/registry/v3/system/info", "http://apicurio", "http://xml"))
            .isEqualTo("http://apicurio");
        assertThat(ProxyResource.upstreamBase("/apis/registry/v2/search/artifacts", "http://apicurio", "http://xml"))
            .isEqualTo("http://apicurio");
        assertThat(ProxyResource.upstreamBase("/apis/ccompat/v7/subjects", "http://apicurio", "http://xml"))
            .isEqualTo("http://apicurio");
    }

    @Test
    void otherPathsAreRoutedToTheXmlSchemaService() {
        assertThat(ProxyResource.upstreamBase("/schemas/orders", "http://apicurio", "http://xml"))
            .isEqualTo("http://xml");
    }

    // ── RBAC enforcement via HTTP ─────────────────────────────────────────────

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamIsBlockedFromInvoices() {
        given()
            .get("/apis/registry/v3/groups/default/artifacts/invoices")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "bob", roles = {"invoices-team"})
    void invoicesTeamIsBlockedFromOrders() {
        given()
            .get("/apis/registry/v3/groups/default/artifacts/orders")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamPassesPolicyForOrders() {
        // Upstream not running → 502, but crucially not 403 (policy allowed)
        int status = given()
            .get("/apis/registry/v3/groups/default/artifacts/orders")
            .then()
            .extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "bob", roles = {"invoices-team"})
    void invoicesTeamPassesPolicyForInvoices() {
        int status = given()
            .get("/apis/registry/v3/groups/default/artifacts/invoices")
            .then()
            .extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "schemadmin", roles = {"schema-admin"})
    void schemaAdminPassesPolicyForAnything() {
        int ordersStatus = given()
            .get("/apis/registry/v3/groups/default/artifacts/orders")
            .then().extract().statusCode();
        int invoicesStatus = given()
            .get("/apis/registry/v3/groups/default/artifacts/invoices")
            .then().extract().statusCode();
        assertThat(ordersStatus).isNotEqualTo(403);
        assertThat(invoicesStatus).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ordersTeamCannotDeleteOrders() {
        // DELETE = DELETE action; orders-team only has READ, WRITE
        given()
            .delete("/apis/registry/v3/groups/default/artifacts/orders")
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
            .get("/apis/registry/v3/ids/globalIds/7")
            .then()
            .statusCode(403);
    }

    @Test
    @TestSecurity(user = "schemadmin", roles = {"schema-admin"})
    void byIdLookupAllowedForWildcardRole() {
        // schema-admin holds artifacts: ["*"], so the by-id lookup passes policy
        // (non-403; 502 because the upstream registry is not running in tests).
        int status = given()
            .get("/apis/registry/v3/ids/globalIds/7")
            .then().extract().statusCode();
        assertThat(status).isNotEqualTo(403);
    }

    // ── mTLS identities are authorized from Kafka ACLs ────────────────────────

    @Inject KafkaAclPolicySource aclSource;

    private void aclSnapshot(AclBinding... bindings) {
        aclSource.replaceSnapshot(java.util.List.of(bindings));
    }

    private static AclBinding topicAcl(String principal, String topic, AclOperation op, AclPermissionType perm) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + principal, "*", op, perm));
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceWithWriteAclPassesPolicyForItsSubjects() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        int put = given().body("{}").contentType("application/json")
            .put("/apis/registry/v3/groups/default/artifacts/orders-value")
            .then().extract().statusCode();
        assertThat(put).isNotEqualTo(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void v3CreateIsAuthorizedOnTheArtifactIdInTheBody() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        int own = given().body("{\"artifactId\":\"orders-value\",\"artifactType\":\"AVRO\"}")
            .contentType("application/json")
            .post("/apis/registry/v3/groups/default/artifacts?ifExists=FIND_OR_CREATE_VERSION")
            .then().extract().statusCode();
        assertThat(own).isNotEqualTo(403);
        given().body("{\"artifactId\":\"invoices-value\",\"artifactType\":\"AVRO\"}")
            .contentType("application/json")
            .post("/apis/registry/v3/groups/default/artifacts")
            .then().statusCode(403);
        // No artifact id in the body: the registry would pick one, so this is registry-wide.
        given().body("{}").contentType("application/json")
            .post("/apis/registry/v3/groups/default/artifacts")
            .then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void v2CreateIsAuthorizedOnTheArtifactIdHeader() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        int own = given().body("{}").contentType("application/json").header("X-Registry-ArtifactId", "orders-value")
            .post("/apis/registry/v2/groups/default/artifacts")
            .then().extract().statusCode();
        assertThat(own).isNotEqualTo(403);
        given().body("{}").contentType("application/json")
            .post("/apis/registry/v2/groups/default/artifacts")
            .then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-reader", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void v3SearchByContentNeedsOnlyReadOnTheArtifact() {
        aclSnapshot(topicAcl("CN=orders-reader", "orders", AclOperation.READ, AclPermissionType.ALLOW));
        int own = given().body("{}").contentType("application/json")
            .post("/apis/registry/v3/search/versions?artifactId=orders-value&groupId=default")
            .then().extract().statusCode();
        assertThat(own).isNotEqualTo(403);
        given().body("{}").contentType("application/json")
            .post("/apis/registry/v3/search/versions?artifactId=invoices-value&groupId=default")
            .then().statusCode(403);
    }

    // ── ccompat (Confluent serdes): subject = artifact ────────────────────────

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void ccompatRegisterIsAuthorizedOnTheSubject() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        int own = given().body("{\"schema\":\"{}\"}").contentType("application/vnd.schemaregistry.v1+json")
            .post("/apis/ccompat/v7/subjects/orders-value/versions")
            .then().extract().statusCode();
        assertThat(own).isNotEqualTo(403);
        given().body("{\"schema\":\"{}\"}").contentType("application/vnd.schemaregistry.v1+json")
            .post("/apis/ccompat/v7/subjects/invoices-value/versions")
            .then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-reader", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void ccompatSchemaLookupNeedsOnlyReadOnTheSubject() {
        aclSnapshot(topicAcl("CN=orders-reader", "orders", AclOperation.READ, AclPermissionType.ALLOW));
        int lookup = given().body("{\"schema\":\"{}\"}").contentType("application/vnd.schemaregistry.v1+json")
            .post("/apis/ccompat/v7/subjects/orders-value")
            .then().extract().statusCode();
        assertThat(lookup).isNotEqualTo(403);
        given().body("{\"schema\":\"{}\"}").contentType("application/vnd.schemaregistry.v1+json")
            .post("/apis/ccompat/v7/subjects/orders-value/versions")
            .then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void ccompatSchemaIdIsNotAuthorizedByTheSubjectHint() {
        // The registry is unreachable in tests, so id 7 resolves to nothing and needs "*".
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        given().get("/apis/ccompat/v7/schemas/ids/7?subject=orders-value").then().statusCode(403);
    }

    // ── denials are answered in the registry's own error format ───────────────

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void v3DenialIsAProblemDetailsDocument() {
        given()
            .get("/apis/registry/v3/groups/default/artifacts/invoices")
            .then()
            .statusCode(403)
            .contentType("application/json")
            .body("status", is(403))
            .body("title", is("Forbidden"))
            .body("detail", is("READ on artifact 'invoices' is not permitted for user:alice"));
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void ccompatDenialUsesSchemaRegistryErrorCode() {
        given()
            .get("/apis/ccompat/v7/subjects/invoices/versions/latest")
            .then()
            .statusCode(403)
            .contentType("application/vnd.schemaregistry.v1+json")
            .body("error_code", is(40301));
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceIsBlockedFromOtherTopicsSubjects() {
        aclSnapshot(topicAcl("CN=orders-service", "orders", AclOperation.WRITE, AclPermissionType.ALLOW));
        given().get("/apis/registry/v3/groups/default/artifacts/invoices-value").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceWithDenyIsRefused() {
        aclSnapshot(topicAcl("*", "*", AclOperation.ALL, AclPermissionType.ALLOW),
                    topicAcl("CN=orders-service", "orders", AclOperation.ALL, AclPermissionType.DENY));
        given().get("/apis/registry/v3/groups/default/artifacts/orders-value").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=orders-service", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceWithoutMappingIgnoresRoleFile() {
        aclSnapshot(); // no ACLs at all; "orders" is granted to orders-team in the role file
        given().get("/apis/registry/v3/groups/default/artifacts/orders").then().statusCode(403);
    }

    @Test
    @TestSecurity(user = "CN=payments-app", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void mtlsServiceMappedToRoleGetsThatRolesRules() {
        aclSnapshot(); // no ACLs; test-policy.yaml maps CN=payments-app → invoices-team
        int status = given().get("/apis/registry/v3/groups/default/artifacts/invoices")
            .then().extract().statusCode();
        assertThat(status).isNotEqualTo(403);
        given().get("/apis/registry/v3/groups/default/artifacts/orders").then().statusCode(403);
    }

    // ── audit principal through the CDI proxy (regression: field access on a client proxy) ──

    @Inject ProxyResource resource;

    @Test
    @TestSecurity(user = "CN=orders-service,O=Acme", attributes = @SecurityAttribute(key = "proxy.auth", value = "mtls"))
    void auditPrincipalForMtlsIdentityUsesConfiguredDnMode() {
        assertThat(resource.auditPrincipal()).isEqualTo("user:CN=orders-service,O=Acme");
    }

    @Test
    @TestSecurity(user = "alice", roles = {"orders-team"})
    void auditPrincipalForOidcIdentityIsTheUserName() {
        assertThat(resource.auditPrincipal()).isEqualTo("user:alice");
    }
}
