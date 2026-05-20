package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyCustomFilter;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyFiltersConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyXmlFilterConfig;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KroxyliciousConfigBuilderTest {

    private static final String NS = "kafka";
    private static final String PROXY_NAME = "my-proxy";
    private static final String POOL_REF = "brokers-a";
    private static final String CLUSTER_REF = "kafka-a";
    private static final int CLIENT_PORT = 9094;

    private KroxyliciousConfigBuilder builder;

    @BeforeEach
    void setup() {
        builder = new KroxyliciousConfigBuilder();
    }

    @Test
    void minimalConfig_noTls_noFilters() {
        String cfg = builder.build(proxy(), 3, 0, NS);

        assertThat(cfg).contains("virtualClusters:");
        assertThat(cfg).contains("bootstrapAddress: 0.0.0.0:" + CLIENT_PORT);
        assertThat(cfg).contains(POOL_REF + "-headless." + NS + ".svc.cluster.local:9092");
        assertThat(cfg).doesNotContain("defaultFilters:");
        assertThat(cfg).doesNotContain("oauth-bearer-validation");
        assertThat(cfg).doesNotContain("authorization");
    }

    @Test
    void brokerNodeIdRanges_usesRanges() {
        KafkaProxy p = proxy();
        BrokerNodeIdRange r1 = range("cluster-a", 0, 2);
        BrokerNodeIdRange r2 = range("cluster-b", 1000, 1002);
        p.getSpec().setBrokerNodeIdRanges(List.of(r1, r2));

        String cfg = builder.build(p, 3, 0, NS);

        assertThat(cfg).contains("- name: cluster-a");
        assertThat(cfg).contains("start: 0");
        assertThat(cfg).contains("end: 2");
        assertThat(cfg).contains("- name: cluster-b");
        assertThat(cfg).contains("start: 1000");
        assertThat(cfg).contains("end: 1002");
    }

    @Test
    void noBrokerNodeIdRanges_usesBrokerCount() {
        String cfg = builder.build(proxy(), 3, 500, NS);

        assertThat(cfg).contains("start: 500");
        assertThat(cfg).contains("end: 502");  // 500 + 3 - 1
    }

    @Test
    void oidcEnabled_addsThreeFiltersInOrder() {
        KafkaProxy p = proxy();
        p.getSpec().setOidc(oidc("https://keycloak/certs", null, null));

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).contains("jwt-groups");
        assertThat(cfg).contains("oauth-bearer-validation");
        assertThat(cfg).contains("sasl-handshake-synthesizer");
        assertThat(cfg).contains("defaultFilters:");

        // verify order in defaultFilters section: oauth validates first, then jwt, then sasl
        int oauthIdx = cfg.indexOf("- oauth-bearer-validation");
        int jwtIdx = cfg.indexOf("- jwt-groups");
        int saslIdx = cfg.indexOf("- sasl-handshake-synthesizer");
        assertThat(oauthIdx).isLessThan(jwtIdx);
        assertThat(jwtIdx).isLessThan(saslIdx);
    }

    @Test
    void oidcEnabled_optionalFieldsAbsentWhenNull() {
        KafkaProxy p = proxy();
        p.getSpec().setOidc(oidc("https://keycloak/certs", null, null));

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).doesNotContain("expectedIssuer:");
        assertThat(cfg).doesNotContain("expectedAudience:");
    }

    @Test
    void oidcEnabled_optionalFieldsPresentWhenSet() {
        KafkaProxy p = proxy();
        p.getSpec().setOidc(oidc("https://keycloak/certs", "https://issuer", "my-audience"));

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).contains("expectedIssuer: https://issuer");
        assertThat(cfg).contains("expectedAudience: my-audience");
    }

    @Test
    void rbacRef_addsAuthorizationFilter() {
        KafkaProxy p = proxy();
        p.getSpec().setRbacRef("my-rbac");

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).contains("- name: authorization");
        assertThat(cfg).contains("GroupAwareAuthorizerService");
        assertThat(cfg).contains("- authorization");
    }

    @Test
    void xmlValidation_addsXmlFilter() {
        KafkaProxy p = proxy();
        KafkaProxyXmlFilterConfig xmlCfg = new KafkaProxyXmlFilterConfig();
        xmlCfg.setEnabled(true);
        xmlCfg.setSchemaTopic("xml-schemas");
        p.getSpec().getFilters().setXmlValidation(xmlCfg);

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).contains("- name: xml-validation");
        assertThat(cfg).contains("schemaTopic: xml-schemas");
        assertThat(cfg).contains(POOL_REF + "-headless." + NS + ".svc.cluster.local:9092");
        assertThat(cfg).contains("- xml-validation");
    }

    @Test
    void customFilter_appendedWithConfig() {
        KafkaProxy p = proxy();
        KafkaProxyCustomFilter custom = new KafkaProxyCustomFilter();
        custom.setName("my-filter");
        custom.setType("MyFilterFactory");
        custom.setConfig(Map.of("key1", "val1"));
        p.getSpec().setCustomFilters(List.of(custom));

        String cfg = builder.build(p, 1, 0, NS);

        assertThat(cfg).contains("- name: my-filter");
        assertThat(cfg).contains("type: MyFilterFactory");
        assertThat(cfg).contains("key1: val1");
        assertThat(cfg).contains("- my-filter");
    }

    @Test
    void filterOrder_allEnabled_correctDefaultFiltersOrder() {
        KafkaProxy p = proxy();
        p.getSpec().setOidc(oidc("https://keycloak/certs", null, null));
        p.getSpec().setRbacRef("my-rbac");
        KafkaProxyXmlFilterConfig xmlCfg = new KafkaProxyXmlFilterConfig();
        xmlCfg.setEnabled(true);
        xmlCfg.setSchemaTopic("schemas");
        p.getSpec().getFilters().setXmlValidation(xmlCfg);

        String cfg = builder.build(p, 1, 0, NS);

        // Extract defaultFilters section
        int defaultFiltersIdx = cfg.indexOf("\ndefaultFilters:");
        assertThat(defaultFiltersIdx).isGreaterThan(0);
        String defaultSection = cfg.substring(defaultFiltersIdx);

        int jwt = defaultSection.indexOf("- jwt-groups");
        int oauth = defaultSection.indexOf("- oauth-bearer-validation");
        int sasl = defaultSection.indexOf("- sasl-handshake-synthesizer");
        int auth = defaultSection.indexOf("- authorization");
        int xml = defaultSection.indexOf("- xml-validation");

        assertThat(oauth).isLessThan(jwt);
        assertThat(jwt).isLessThan(sasl);
        assertThat(sasl).isLessThan(auth);
        assertThat(auth).isLessThan(xml);
    }

    @Test
    void alwaysAddsTargetAndGatewayMtls() {
        // The config always emits target (proxy→broker) mTLS at /etc/proxy/kafka-tls
        // and gateway (client→proxy) mTLS at /etc/proxy/server-tls — paths are fixed by
        // ProxyDeploymentBuilder's volume mounts, regardless of any KafkaProxyTlsConfig
        // override (the override only changes the SECRET behind those mounts).
        String cfg = builder.build(proxy(), 1, 0, NS);

        // Target (upstream to brokers)
        assertThat(cfg).contains("/etc/proxy/kafka-tls/tls.key");
        assertThat(cfg).contains("/etc/proxy/kafka-tls/tls.crt");
        assertThat(cfg).contains("/etc/proxy/kafka-tls/ca.crt");
        // Gateway (downstream from clients)
        assertThat(cfg).contains("/etc/proxy/server-tls/tls.key");
        assertThat(cfg).contains("/etc/proxy/server-tls/tls.crt");
        assertThat(cfg).contains("/etc/proxy/server-tls/ca.crt");
    }

    // --- helpers ---

    private KafkaProxy proxy() {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(PROXY_NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setPoolRef(POOL_REF);
        spec.setClusterRef(CLUSTER_REF);
        spec.setClientPort(CLIENT_PORT);
        spec.setReplicas(1);
        p.setSpec(spec);
        return p;
    }

    private BrokerNodeIdRange range(String name, int start, int end) {
        BrokerNodeIdRange r = new BrokerNodeIdRange();
        r.setName(name);
        r.setStart(start);
        r.setEnd(end);
        return r;
    }

    private KafkaProxyOidcConfig oidc(String jwksUrl, String issuer, String audience) {
        KafkaProxyOidcConfig o = new KafkaProxyOidcConfig();
        o.setJwksEndpointUrl(jwksUrl);
        o.setExpectedIssuer(issuer);
        o.setExpectedAudience(audience);
        return o;
    }
}
