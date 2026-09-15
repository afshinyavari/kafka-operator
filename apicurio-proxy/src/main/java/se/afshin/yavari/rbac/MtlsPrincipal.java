package se.afshin.yavari.rbac;

import io.quarkus.security.credential.CertificateCredential;
import io.quarkus.security.identity.SecurityIdentity;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.cert.X509Certificate;

/** Derives the Kafka principal name from an mTLS client certificate. */
public final class MtlsPrincipal {

    /** {@code DN}: RFC 2253 subject (Strimzi KafkaUser default, e.g. {@code CN=orders-service}).
     *  {@code CN}: only the common name, for clusters using {@code ssl.principal.mapping.rules}. */
    public enum Mode { DN, CN }

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
}
