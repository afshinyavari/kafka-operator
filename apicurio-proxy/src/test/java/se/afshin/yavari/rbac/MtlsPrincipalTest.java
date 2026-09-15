package se.afshin.yavari.rbac;

import io.quarkus.security.runtime.QuarkusPrincipal;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
