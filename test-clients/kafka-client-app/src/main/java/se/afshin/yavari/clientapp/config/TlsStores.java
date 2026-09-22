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
    private static final java.util.Set<String> VALID_TYPES = java.util.Set.of("PKCS12", "JKS");

    /** Backward-compatible variant that discards its problems; prefer the 3-arg overload. */
    public static TlsStores resolve(Env env, String prefix) {
        return resolve(env, prefix, new Problems());
    }

    public static TlsStores resolve(Env env, String prefix, Problems problems) {
        String keystorePath = lookup(env, prefix, "KEYSTORE_PATH", null);
        String keystorePassword = lookup(env, prefix, "KEYSTORE_PASSWORD", null);
        String keystoreType = lookup(env, prefix, "KEYSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT);
        String truststorePath = lookup(env, prefix, "TRUSTSTORE_PATH", null);
        String truststorePassword = lookup(env, prefix, "TRUSTSTORE_PASSWORD", null);
        String truststoreType = lookup(env, prefix, "TRUSTSTORE_TYPE", DEFAULT_TYPE).toUpperCase(Locale.ROOT);

        if (keystorePath != null && keystorePassword == null) {
            problems.add(prefix + "KEYSTORE_PASSWORD (or TLS_KEYSTORE_PASSWORD) is required when a keystore path is set");
        }
        if (truststorePath != null && truststorePassword == null) {
            problems.add(prefix + "TRUSTSTORE_PASSWORD (or TLS_TRUSTSTORE_PASSWORD) is required when a truststore path is set");
        }
        if (!VALID_TYPES.contains(keystoreType)) {
            problems.add(prefix + "KEYSTORE_TYPE must be PKCS12 or JKS, got '" + keystoreType + "'");
        }
        if (!VALID_TYPES.contains(truststoreType)) {
            problems.add(prefix + "TRUSTSTORE_TYPE must be PKCS12 or JKS, got '" + truststoreType + "'");
        }

        return new TlsStores(keystorePath, keystorePassword, keystoreType,
                truststorePath, truststorePassword, truststoreType);
    }

    private static String lookup(Env env, String prefix, String suffix, String def) {
        return env.get(prefix + suffix).or(() -> env.get(GLOBAL_PREFIX + suffix)).orElse(def);
    }

    public boolean hasKeystore() { return keystorePath != null; }
    public boolean hasTruststore() { return truststorePath != null; }
}
