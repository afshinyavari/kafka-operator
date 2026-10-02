package se.afshin.yavari.rbac;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.rbac.RegistryRequestClassifier.Api;
import se.afshin.yavari.rbac.RegistryRequestClassifier.Classified;
import se.afshin.yavari.rbac.RegistryRequestClassifier.IdLookup;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static se.afshin.yavari.rbac.PolicyEngine.Action.*;

class RegistryRequestClassifierTest {

    private static final String V3 = "/apis/registry/v3";
    private static final String V2 = "/apis/registry/v2";
    private static final String CC = "/apis/ccompat/v7";
    private static final String JSON = "application/json";

    private static Classified classify(String method, String pathAndQuery) {
        int q = pathAndQuery.indexOf('?');
        String path = q < 0 ? pathAndQuery : pathAndQuery.substring(0, q);
        String query = q < 0 ? null : pathAndQuery.substring(q + 1);
        return RegistryRequestClassifier.classify(method, path, query, null, null, null);
    }

    private static Classified create(String path, String contentType, String idHeader, String body) {
        return RegistryRequestClassifier.classify("POST", path, null, contentType, idHeader,
                body == null ? null : body.getBytes(StandardCharsets.UTF_8));
    }

    private static String createBody(String artifactId) {
        return "{\"artifactId\":\"" + artifactId + "\",\"artifactType\":\"AVRO\","
                + "\"firstVersion\":{\"content\":{\"content\":\"{}\",\"contentType\":\"application/json\"}}}";
    }

    // ── core v3: artifact in the path ─────────────────────────────────────────

    @Test
    void v3ArtifactPathNamesTheArtifact() {
        Classified c = classify("GET", V3 + "/groups/default/artifacts/orders-value");
        assertThat(c.api()).isEqualTo(Api.CORE_V3);
        assertThat(c.artifact()).isEqualTo("orders-value");
        assertThat(c.action()).isEqualTo(READ);
        assertThat(c.lookup()).isNull();
    }

    @Test
    void v3VersionSubPathsBelongToTheArtifact() {
        assertThat(classify("GET", V3 + "/groups/default/artifacts/orders-value/versions/branch%3Dlatest/content")
                .artifact()).isEqualTo("orders-value");
        assertThat(classify("POST", V3 + "/groups/team-a/artifacts/orders-value/versions").artifact())
                .isEqualTo("orders-value");
    }

    @Test
    void actionFollowsTheHttpMethod() {
        String path = V3 + "/groups/default/artifacts/orders-value";
        assertThat(classify("GET", path).action()).isEqualTo(READ);
        assertThat(classify("HEAD", path).action()).isEqualTo(READ);
        assertThat(classify("PUT", path).action()).isEqualTo(WRITE);
        assertThat(classify("POST", path + "/versions").action()).isEqualTo(WRITE);
        assertThat(classify("DELETE", path).action()).isEqualTo(DELETE);
    }

    @Test
    void v3ListingArtifactsIsRegistryWide() {
        assertThat(classify("GET", V3 + "/groups/default/artifacts").artifact()).isEqualTo("*");
        assertThat(classify("DELETE", V3 + "/groups/default/artifacts").artifact()).isEqualTo("*");
        assertThat(classify("GET", V3 + "/groups/default").artifact()).isEqualTo("*");
    }

    @Test
    void groupNamedArtifactsDoesNotShiftTheArtifactPosition() {
        assertThat(classify("GET", V3 + "/groups/artifacts/artifacts/secret-value").artifact())
                .isEqualTo("secret-value");
    }

    @Test
    void encodedSlashStaysInsideTheArtifactSegment() {
        assertThat(classify("GET", V3 + "/groups/default/artifacts/orders-value%2F..%2Fsecret-value").artifact())
                .isEqualTo("orders-value/../secret-value");
    }

    @Test
    void v3SystemAndAdminPathsAreRegistryWide() {
        assertThat(classify("GET", V3 + "/system/info").artifact()).isEqualTo("*");
        assertThat(classify("POST", V3 + "/admin/rules").artifact()).isEqualTo("*");
    }

    @Test
    void futureCoreVersionsFollowV3Rules() {
        Classified c = classify("GET", "/apis/registry/v4/groups/default/artifacts/orders-value");
        assertThat(c.api()).isEqualTo(Api.CORE_V3);
        assertThat(c.artifact()).isEqualTo("orders-value");
    }

    // ── core v3: create, artifact id in the JSON body ─────────────────────────

    @Test
    void v3CreateTakesTheArtifactIdFromTheBody() {
        Classified c = create(V3 + "/groups/default/artifacts", JSON, null, createBody("payments-value"));
        assertThat(c.artifact()).isEqualTo("payments-value");
        assertThat(c.action()).isEqualTo(WRITE);
    }

    @Test
    void v3CreateAcceptsJsonContentTypeWithParametersAndAnyCase() {
        assertThat(create(V3 + "/groups/default/artifacts", "application/json; charset=UTF-8", null,
                createBody("payments-value")).artifact()).isEqualTo("payments-value");
        assertThat(create(V3 + "/groups/default/artifacts", "APPLICATION/JSON", null,
                createBody("payments-value")).artifact()).isEqualTo("payments-value");
    }

    @Test
    void v3CreateWithNonJsonContentTypeIsRegistryWide() {
        assertThat(create(V3 + "/groups/default/artifacts", "text/plain", null, createBody("payments-value"))
                .artifact()).isEqualTo("*");
        assertThat(create(V3 + "/groups/default/artifacts", null, null, createBody("payments-value"))
                .artifact()).isEqualTo("*");
    }

    @Test
    void v3CreateWithDuplicateArtifactIdKeyIsRegistryWide() {
        // The registry keeps the last duplicate; never authorize on the first.
        String body = "{\"artifactId\":\"payments-value\",\"artifactId\":\"orders-value\"}";
        assertThat(create(V3 + "/groups/default/artifacts", JSON, null, body).artifact()).isEqualTo("*");
    }

    @Test
    void v3CreateWithoutUsableArtifactIdIsRegistryWide() {
        String path = V3 + "/groups/default/artifacts";
        assertThat(create(path, JSON, null, "{\"artifactType\":\"AVRO\"}").artifact()).isEqualTo("*");
        assertThat(create(path, JSON, null, "{\"artifactId\":42}").artifact()).isEqualTo("*");
        assertThat(create(path, JSON, null, "{\"artifactId\":\"\"}").artifact()).isEqualTo("*");
        assertThat(create(path, JSON, null, "{\"firstVersion\":{\"artifactId\":\"payments-value\"}}").artifact())
                .isEqualTo("*");
        assertThat(create(path, JSON, null, "not json").artifact()).isEqualTo("*");
        assertThat(create(path, JSON, null, "{\"artifactId\":\"payments-value\"").artifact()).isEqualTo("*");
        assertThat(create(path, JSON, null, null).artifact()).isEqualTo("*");
    }

    @Test
    void v3CreateIgnoresTheV2ArtifactIdHeader() {
        assertThat(create(V3 + "/groups/default/artifacts", JSON, "orders-value", createBody("payments-value"))
                .artifact()).isEqualTo("payments-value");
        assertThat(create(V3 + "/groups/default/artifacts", JSON, "orders-value", "{}").artifact()).isEqualTo("*");
    }

    // ── core v3: search ───────────────────────────────────────────────────────

    @Test
    void v3SearchByContentIsAReadOnTheNamedArtifact() {
        Classified c = classify("POST", V3 + "/search/versions?artifactId=payments-value&artifactType=AVRO"
                + "&canonical=false&groupId=default&limit=100&order=desc&orderby=globalId");
        assertThat(c.artifact()).isEqualTo("payments-value");
        assertThat(c.action()).isEqualTo(READ);
    }

    @Test
    void v3SearchWithArtifactIdFilterIsScopedToThatArtifact() {
        assertThat(classify("GET", V3 + "/search/versions?artifactId=payments-value").artifact())
                .isEqualTo("payments-value");
        assertThat(classify("GET", V3 + "/search/artifacts?limit=5&artifactId=payments%2Dvalue").artifact())
                .isEqualTo("payments-value");
    }

    @Test
    void v3SearchWithRepeatedArtifactIdFilterIsRegistryWide() {
        assertThat(classify("GET", V3 + "/search/versions?artifactId=payments-value&artifactId=orders-value")
                .artifact()).isEqualTo("*");
    }

    @Test
    void v3GeneralSearchIsARegistryWideRead() {
        Classified c = classify("POST", V3 + "/search/versions?artifactType=AVRO");
        assertThat(c.artifact()).isEqualTo("*");
        assertThat(c.action()).isEqualTo(READ);
        assertThat(classify("GET", V3 + "/search/artifacts?name=orders").artifact()).isEqualTo("*");
        assertThat(classify("GET", V3 + "/search/groups").artifact()).isEqualTo("*");
    }

    // ── core: id-only requests carry a lookup ─────────────────────────────────

    @Test
    void v3GlobalIdPathCarriesALookup() {
        Classified c = classify("GET", V3 + "/ids/globalIds/7");
        assertThat(c.lookup()).isEqualTo(new IdLookup(Api.CORE_V3, V3, "globalId", "7"));
        assertThat(c.artifact()).isEqualTo("*");
        assertThat(c.action()).isEqualTo(READ);
    }

    @Test
    void v3ContentIdReferencesPathCarriesALookup() {
        assertThat(classify("GET", V3 + "/ids/contentIds/42/references").lookup())
                .isEqualTo(new IdLookup(Api.CORE_V3, V3, "contentId", "42"));
    }

    @Test
    void searchByIdCarriesALookup() {
        assertThat(classify("GET", V3 + "/search/versions?globalId=11").lookup())
                .isEqualTo(new IdLookup(Api.CORE_V3, V3, "globalId", "11"));
        assertThat(classify("GET", V3 + "/search/artifacts?limit=1&contentId=5").lookup())
                .isEqualTo(new IdLookup(Api.CORE_V3, V3, "contentId", "5"));
    }

    @Test
    void idsThatAreNotPlainNumbersCarryNoLookup() {
        assertThat(classify("GET", V3 + "/ids/globalIds/7%26contentId%3D1").lookup()).isNull();
        assertThat(classify("GET", V3 + "/ids/contentHashes/abc123").lookup()).isNull();
        assertThat(classify("GET", V3 + "/ids/contentHashes/abc123").artifact()).isEqualTo("*");
        assertThat(classify("GET", V3 + "/search/versions?globalId=abc").lookup()).isNull();
        assertThat(classify("GET", V3 + "/search/versions?globalId=1&globalId=2").lookup()).isNull();
    }

    // ── core v2 ───────────────────────────────────────────────────────────────

    @Test
    void v2ArtifactPathNamesTheArtifact() {
        Classified c = classify("GET", V2 + "/groups/default/artifacts/orders/versions/1");
        assertThat(c.api()).isEqualTo(Api.CORE_V2);
        assertThat(c.artifact()).isEqualTo("orders");
    }

    @Test
    void v2CreateTakesTheArtifactIdFromTheHeader() {
        assertThat(create(V2 + "/groups/default/artifacts", JSON, "orders-value", "{}").artifact())
                .isEqualTo("orders-value");
        assertThat(create(V2 + "/groups/default/artifacts", JSON, null, createBody("orders-value")).artifact())
                .isEqualTo("*");
        assertThat(create(V2 + "/groups/default/artifacts", JSON, " ", "{}").artifact()).isEqualTo("*");
    }

    @Test
    void v2IdRequestsCarryAV2Lookup() {
        assertThat(classify("GET", V2 + "/ids/globalIds/7/references").lookup())
                .isEqualTo(new IdLookup(Api.CORE_V2, V2, "globalId", "7"));
        assertThat(classify("GET", V2 + "/search/artifacts?globalId=11").lookup())
                .isEqualTo(new IdLookup(Api.CORE_V2, V2, "globalId", "11"));
    }

    @Test
    void v2SearchHasNoArtifactIdFilter() {
        assertThat(classify("GET", V2 + "/search/artifacts?artifactId=orders-value").artifact()).isEqualTo("*");
    }

    // ── ccompat (Confluent Schema Registry API) ───────────────────────────────

    @Test
    void ccompatRegisterIsAWriteOnTheSubject() {
        Classified c = classify("POST", CC + "/subjects/shipments-value/versions?normalize=false");
        assertThat(c.api()).isEqualTo(Api.CCOMPAT);
        assertThat(c.artifact()).isEqualTo("shipments-value");
        assertThat(c.action()).isEqualTo(WRITE);
    }

    @Test
    void ccompatSchemaLookupUnderSubjectIsARead() {
        Classified c = classify("POST", CC + "/subjects/shipments-value?normalize=false&deleted=false");
        assertThat(c.artifact()).isEqualTo("shipments-value");
        assertThat(c.action()).isEqualTo(READ);
    }

    @Test
    void ccompatSubjectPathsNameTheSubject() {
        assertThat(classify("GET", CC + "/subjects/shipments-value/versions/latest").artifact())
                .isEqualTo("shipments-value");
        Classified delete = classify("DELETE", CC + "/subjects/shipments-value");
        assertThat(delete.artifact()).isEqualTo("shipments-value");
        assertThat(delete.action()).isEqualTo(DELETE);
        assertThat(classify("GET", CC + "/subjects").artifact()).isEqualTo("*");
    }

    @Test
    void ccompatSchemaIdCarriesALookupAndIgnoresTheSubjectHint() {
        Classified c = classify("GET", CC + "/schemas/ids/6?fetchMaxId=false&subject=shipments-value");
        assertThat(c.lookup()).isEqualTo(new IdLookup(Api.CCOMPAT, CC, "id", "6"));
        assertThat(c.artifact()).isEqualTo("*");
        assertThat(classify("GET", CC + "/schemas/ids/6/versions").lookup())
                .isEqualTo(new IdLookup(Api.CCOMPAT, CC, "id", "6"));
        assertThat(classify("GET", CC + "/schemas/ids/abc").lookup()).isNull();
        assertThat(classify("GET", CC + "/schemas/types").artifact()).isEqualTo("*");
    }

    @Test
    void ccompatLookupUsesTheRequestedApiVersion() {
        assertThat(classify("GET", "/apis/ccompat/v8/schemas/ids/6").lookup())
                .isEqualTo(new IdLookup(Api.CCOMPAT, "/apis/ccompat/v8", "id", "6"));
    }

    @Test
    void ccompatCompatibilityCheckIsAReadOnTheSubject() {
        Classified c = classify("POST", CC + "/compatibility/subjects/shipments-value/versions/latest");
        assertThat(c.artifact()).isEqualTo("shipments-value");
        assertThat(c.action()).isEqualTo(READ);
    }

    @Test
    void ccompatConfigAndModeAreScopedToTheSubject() {
        assertThat(classify("GET", CC + "/config/shipments-value").artifact()).isEqualTo("shipments-value");
        Classified put = classify("PUT", CC + "/config/shipments-value");
        assertThat(put.artifact()).isEqualTo("shipments-value");
        assertThat(put.action()).isEqualTo(WRITE);
        assertThat(classify("GET", CC + "/mode/shipments-value").artifact()).isEqualTo("shipments-value");
        assertThat(classify("PUT", CC + "/config").artifact()).isEqualTo("*");
        assertThat(classify("GET", CC + "/mode").artifact()).isEqualTo("*");
    }

    @Test
    void ccompatTopicAssociationLookupMapsToTheTopicsSubject() {
        // Sent by Confluent clients 8.x before every (de)serialization.
        Classified c = classify("GET", CC + "/associations/resources/-/shipments?resourceType=topic&associationType=value");
        assertThat(c.artifact()).isEqualTo("shipments-value");
        assertThat(c.action()).isEqualTo(READ);
        assertThat(classify("GET", CC + "/associations/resources/-/shipments?resourceType=topic&associationType=key")
                .artifact()).isEqualTo("shipments-key");
        assertThat(classify("GET", CC + "/associations/resources/-/shipments").artifact()).isEqualTo("*");
    }

    // ── everything else (XML schema service) ──────────────────────────────────

    @Test
    void xmlSchemaPathNamesTheSchema() {
        Classified c = classify("DELETE", "/schemas/orders");
        assertThat(c.api()).isEqualTo(Api.OTHER);
        assertThat(c.artifact()).isEqualTo("orders");
        assertThat(c.action()).isEqualTo(DELETE);
        assertThat(classify("GET", "/schemas").artifact()).isEqualTo("*");
        assertThat(classify("GET", "/").artifact()).isEqualTo("*");
    }
}
