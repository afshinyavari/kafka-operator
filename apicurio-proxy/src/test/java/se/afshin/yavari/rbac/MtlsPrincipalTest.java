package se.afshin.yavari.rbac;

import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MtlsPrincipalTest {

    @Test
    void dnModeKeepsRfc2253Subject() {
        assertThat(MtlsPrincipal.of("CN=orders-service,O=Acme", MtlsPrincipal.Mode.DN))
                .isEqualTo("CN=orders-service,O=Acme");
    }

    @Test
    void cnModeExtractsCommonName() {
        assertThat(MtlsPrincipal.of("CN=orders-service,O=Acme", MtlsPrincipal.Mode.CN))
                .isEqualTo("orders-service");
        assertThat(MtlsPrincipal.of("O=Acme,CN=orders-service", MtlsPrincipal.Mode.CN))
                .isEqualTo("orders-service");
    }

    @Test
    void cnModeFallsBackToDnWhenNoCn() {
        assertThat(MtlsPrincipal.of("O=Acme", MtlsPrincipal.Mode.CN)).isEqualTo("O=Acme");
    }

    @Test
    void strimziKafkaUserSubjectIsJustCn() {
        // Strimzi issues "CN=<user>"; Kafka's principal is "User:CN=<user>" → DN mode matches.
        assertThat(MtlsPrincipal.of("CN=orders-service", MtlsPrincipal.Mode.DN)).isEqualTo("CN=orders-service");
    }

    @Test
    void mappingRulesRewriteTheSubjectLikeTheBroker() {
        // Certificates with mandatory O/C fields; the brokers reduce them to "CN=<name>".
        var mapping = MtlsPrincipal.Mapping.ofRules("RULE:^CN=([^,]+),.*$/CN=$1/,DEFAULT");
        assertThat(mapping.apply("CN=kafka-client-app,O=Org,C=SE")).contains("CN=kafka-client-app");
        assertThat(mapping.apply("CN=plain")).contains("CN=plain");
    }

    @Test
    void mappingRuleThatFindsTheCnAnywhereInTheSubject() {
        var mapping = MtlsPrincipal.Mapping.ofRules("RULE:^.*CN=([^,]+).*$/CN=$1/,DEFAULT");
        assertThat(mapping.apply("CN=kafka-client-app,OU=Team,O=Org,C=SE")).contains("CN=kafka-client-app");
        assertThat(mapping.apply("O=Org,CN=kafka-client-app")).contains("CN=kafka-client-app");
        assertThat(mapping.apply("CN=kafka-client-app")).contains("CN=kafka-client-app");
        assertThat(mapping.apply("O=Org")).contains("O=Org");   // DEFAULT
    }

    @Test
    void mappingRulesWithoutAMatchGiveNoPrincipal() {
        var mapping = MtlsPrincipal.Mapping.ofRules("RULE:^CN=([^,]+),O=Org$/CN=$1/");
        assertThat(mapping.apply("CN=kafka-client-app,O=Other")).isEmpty();
    }

    @Test
    void invalidMappingRulesAreRejected() {
        assertThatThrownBy(() -> MtlsPrincipal.Mapping.ofRules("not a rule"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void modeMappingBehavesLikeTheMode() {
        assertThat(MtlsPrincipal.Mapping.of(MtlsPrincipal.Mode.CN).apply("CN=orders-service,O=Acme"))
                .contains("orders-service");
        assertThat(MtlsPrincipal.Mapping.of(MtlsPrincipal.Mode.DN).apply("CN=orders-service,O=Acme"))
                .contains("CN=orders-service,O=Acme");
    }

    @Test
    void identityWithMtlsAttributeIsACertificateIdentity() {
        var mtls = QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal("CN=svc"))
                .addAttribute(MtlsPrincipal.AUTH_ATTRIBUTE, MtlsPrincipal.AUTH_MTLS).build();
        var oidc = QuarkusSecurityIdentity.builder().setPrincipal(new QuarkusPrincipal("alice")).build();
        assertThat(MtlsPrincipal.isCertificateIdentity(mtls)).isTrue();
        assertThat(MtlsPrincipal.isCertificateIdentity(oidc)).isFalse();
        assertThat(MtlsPrincipal.isCertificateIdentity(null)).isFalse();
        assertThat(MtlsPrincipal.principalOf(mtls, MtlsPrincipal.Mode.DN)).isEqualTo("CN=svc");
    }
}
