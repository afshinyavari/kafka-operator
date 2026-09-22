package se.afshin.yavari.clientapp.config;

import java.util.Locale;

/**
 * Keystore/truststore locations. {@link #resolve} looks each variable up under the
 * component prefix first (e.g. {@code KAFKA_TLS_}), then the global {@code TLS_}
 * prefix, then the default. Types default to PKCS12 and are upper-cased.
 */
public record TlsStores(String keystorePath, String keystorePassword, String keystoreType,
                        String truststorePath, String truststorePassword, String truststoreType) {

    public static final String GLOBAL_PREFIX = "TLS_";
    public static final String DEFAULT_TYPE = "PKCS12";

    public static TlsStores resolve(Env env, String prefix) {
        return new TlsStores(
                lookup(env, prefix, "KEYSTORE_PATH", null),
                lookup(env, prefix, "KEYSTORE_PASSWORD", null),
                lookup(env, prefix, "KEYSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT),
                lookup(env, prefix, "TRUSTSTORE_PATH", null),
                lookup(env, prefix, "TRUSTSTORE_PASSWORD", null),
                lookup(env, prefix, "TRUSTSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT));
    }

    private static String lookup(Env env, String prefix, String suffix, String def) {
        return env.get(prefix + suffix).or(() -> env.get(GLOBAL_PREFIX + suffix)).orElse(def);
    }

    public boolean hasKeystore() { return keystorePath != null; }
    public boolean hasTruststore() { return truststorePath != null; }
}
