package se.afshin.yavari.rbac;

import io.quarkus.security.credential.CertificateCredential;
import io.quarkus.security.identity.SecurityIdentity;

import org.apache.kafka.common.security.ssl.SslPrincipalMapper;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Optional;

/** Derives the Kafka principal name from an mTLS client certificate. */
public final class MtlsPrincipal {

    /** {@code DN}: RFC 2253 subject (Strimzi KafkaUser default, e.g. {@code CN=orders-service}).
     *  {@code CN}: only the common name, for clusters using {@code ssl.principal.mapping.rules}. */
    public enum Mode { DN, CN }

    /** How a certificate subject becomes a Kafka principal name: a fixed {@link Mode}, or the
     *  brokers' own {@code ssl.principal.mapping.rules}, evaluated by Kafka's mapper so that the
     *  proxy and the brokers derive the same name. */
    public static final class Mapping {
        private final Mode mode;
        private final SslPrincipalMapper rules;

        private Mapping(Mode mode, SslPrincipalMapper rules) {
            this.mode = mode;
            this.rules = rules;
        }

        public static Mapping of(Mode mode) {
            return new Mapping(mode, null);
        }

        /** @throws IllegalArgumentException when {@code rules} is not a valid rule list */
        public static Mapping ofRules(String rules) {
            return new Mapping(null, SslPrincipalMapper.fromRules(rules));
        }

        /** Empty when no rule matches; Kafka fails authentication for such a certificate. */
        public Optional<String> apply(String rfc2253Dn) {
            if (rules == null) return Optional.of(MtlsPrincipal.of(rfc2253Dn, mode));
            try {
                return Optional.of(rules.getName(rfc2253Dn));
            } catch (IOException noMatchingRule) {
                return Optional.empty();
            }
        }

        @Override
        public String toString() {
            return rules == null ? "mode " + mode : "rules " + rules;
        }
    }

    /** Attribute tests set on a {@code @TestSecurity} identity to mark it as mTLS. */
    static final String AUTH_ATTRIBUTE = "proxy.auth";
    static final String AUTH_MTLS = "mtls";

    private MtlsPrincipal() {}

    public static String of(X509Certificate cert, Mode mode) {
        return of(cert.getSubjectX500Principal().getName(), mode);
    }

    public static String of(String rfc2253Dn, Mode mode) {
        if (mode == Mode.DN) return rfc2253Dn;
        try {
            for (Rdn rdn : new LdapName(rfc2253Dn).getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) return rdn.getValue().toString();
            }
        } catch (Exception ignored) {
            // not a parsable DN — fall through and return it unchanged
        }
        return rfc2253Dn;
    }

    /** True when the request was authenticated with a client certificate. */
    public static boolean isCertificateIdentity(SecurityIdentity id) {
        if (id == null || id.isAnonymous()) return false;
        if (id.getCredential(CertificateCredential.class) != null) return true;
        return AUTH_MTLS.equals(id.getAttribute(AUTH_ATTRIBUTE));
    }

    /** Principal for an mTLS identity: from the certificate when present, otherwise the
     *  identity's principal name (test identities). */
    public static String principalOf(SecurityIdentity id, Mode mode) {
        CertificateCredential cc = id.getCredential(CertificateCredential.class);
        if (cc != null && cc.getCertificate() != null) return of(cc.getCertificate(), mode);
        return of(id.getPrincipal().getName(), mode);
    }

    /** Principal for an mTLS identity under {@code mapping}; empty when no rule matches. */
    public static Optional<String> principalOf(SecurityIdentity id, Mapping mapping) {
        return mapping.apply(principalOf(id, Mode.DN));
    }
}
