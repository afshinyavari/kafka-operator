package se.afshin.yavari.rbac;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns the {@code PROXY_TLS_*} environment into Quarkus TLS-registry + HTTP properties so
 * the proxy can terminate TLS and request client certificates. Registered through
 * {@code META-INF/services}. The keys are runtime config, so reading env at runtime is fine.
 *
 * <pre>
 * PROXY_TLS_ENABLED=true
 * PROXY_TLS_PORT=8443                       (default)
 * PROXY_TLS_KEYSTORE=/tls/server.p12        PROXY_TLS_KEYSTORE_PASSWORD  PROXY_TLS_KEYSTORE_TYPE=PKCS12|JKS|PEM
 *   PEM: PROXY_TLS_CERT=/tls/tls.crt        PROXY_TLS_KEY=/tls/tls.key (PKCS#8)
 * PROXY_TLS_TRUSTSTORE=/tls/clients-ca.p12  PROXY_TLS_TRUSTSTORE_PASSWORD PROXY_TLS_TRUSTSTORE_TYPE
 *   PEM: PROXY_TLS_CA=/tls/ca.crt
 * </pre>
 *
 * Combined with the build-time {@code quarkus.http.ssl.client-auth=request}, a client
 * certificate is optional: bearer-token requests use OIDC, certificate requests use mTLS.
 */
public class ProxyTlsConfigSource implements ConfigSource {

    private final Map<String, String> props;

    public ProxyTlsConfigSource() {
        this.props = properties(System::getenv);
    }

    static Map<String, String> properties(Function<String, String> env) {
        Map<String, String> p = new LinkedHashMap<>();
        if (!"true".equalsIgnoreCase(env.apply("PROXY_TLS_ENABLED"))) return p;

        KafkaSslProps.Store ks = KafkaSslProps.fromEnv(env, "PROXY_TLS");
        if (ks == null) {
            throw new IllegalStateException("PROXY_TLS_ENABLED=true requires PROXY_TLS_KEYSTORE "
                    + "(or PROXY_TLS_CERT + PROXY_TLS_KEY for PEM)");
        }
        if (ks.isPem()) {
            p.put("quarkus.tls.key-store.pem.server.cert", ks.certPath());
            p.put("quarkus.tls.key-store.pem.server.key", ks.keyPath());
        } else {
            String kind = "JKS".equalsIgnoreCase(ks.type()) ? "jks" : "p12";
            p.put("quarkus.tls.key-store." + kind + ".path", ks.path());
            if (ks.password() != null) p.put("quarkus.tls.key-store." + kind + ".password", ks.password());
        }

        KafkaSslProps.Store ts = KafkaSslProps.trustFromEnv(env, "PROXY_TLS");
        if (ts != null) {
            if (ts.isPem()) {
                p.put("quarkus.tls.trust-store.pem.certs", ts.caPath());
            } else {
                String kind = "JKS".equalsIgnoreCase(ts.type()) ? "jks" : "p12";
                p.put("quarkus.tls.trust-store." + kind + ".path", ts.path());
                if (ts.password() != null) p.put("quarkus.tls.trust-store." + kind + ".password", ts.password());
            }
        }

        String port = env.apply("PROXY_TLS_PORT");
        p.put("quarkus.http.ssl-port", port == null || port.isBlank() ? "8443" : port);
        p.put("quarkus.http.insecure-requests", "disabled");
        return p;
    }

    @Override public Map<String, String> getProperties() { return props; }
    @Override public Set<String> getPropertyNames() { return props.keySet(); }
    @Override public String getValue(String key) { return props.get(key); }
    @Override public String getName() { return "proxy-tls-env"; }
    @Override public int getOrdinal() { return 275; }
}
