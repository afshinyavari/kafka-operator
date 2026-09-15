package se.afshin.yavari.rbac;

import org.apache.kafka.common.config.SslConfigs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

/**
 * Translates key material supplied as PKCS12 / JKS files or PEM files into Kafka client
 * {@code ssl.*} properties. Used by both the audit producer and the ACL admin client.
 *
 * <p>Env contract for a keystore with prefix {@code P}: {@code P_KEYSTORE},
 * {@code P_KEYSTORE_PASSWORD}, {@code P_KEYSTORE_TYPE} (PKCS12 default | JKS | PEM); in
 * PEM mode {@code P_CERT} + {@code P_KEY} (PKCS#8) are used instead of a store file.
 * For a truststore: {@code P_TRUSTSTORE}, {@code P_TRUSTSTORE_PASSWORD},
 * {@code P_TRUSTSTORE_TYPE}; in PEM mode {@code P_CA}.
 */
public final class KafkaSslProps {

    /** One store. For PKCS12/JKS {@code path}+{@code password} are set; for PEM the
     *  {@code certPath}/{@code keyPath} (keystore) or {@code caPath} (truststore). */
    public record Store(String type, String path, String password,
                        String certPath, String keyPath, String caPath) {
        public boolean isPem() { return "PEM".equalsIgnoreCase(type); }
    }

    private KafkaSslProps() {}

    public static Store fromEnv(Function<String, String> env, String prefix) {
        String type = orDefault(env.apply(prefix + "_KEYSTORE_TYPE"), "PKCS12");
        String path = env.apply(prefix + "_KEYSTORE");
        String cert = env.apply(prefix + "_CERT");
        String key = env.apply(prefix + "_KEY");
        if (isBlank(path) && (isBlank(cert) || isBlank(key))) return null;
        if (isBlank(path)) type = "PEM";
        return new Store(type, path, env.apply(prefix + "_KEYSTORE_PASSWORD"), cert, key, null);
    }

    public static Store trustFromEnv(Function<String, String> env, String prefix) {
        String type = orDefault(env.apply(prefix + "_TRUSTSTORE_TYPE"), "PKCS12");
        String path = env.apply(prefix + "_TRUSTSTORE");
        String ca = env.apply(prefix + "_CA");
        if (isBlank(path) && isBlank(ca)) return null;
        if (isBlank(path)) type = "PEM";
        return new Store(type, path, env.apply(prefix + "_TRUSTSTORE_PASSWORD"), null, null, ca);
    }

    public static void apply(Properties p, Store keystore, Store truststore) {
        if (keystore != null) {
            if (keystore.isPem()) {
                p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
                p.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, read(keystore.certPath()));
                p.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, read(keystore.keyPath()));
            } else {
                p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, keystore.type().toUpperCase());
                p.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystore.path());
                if (keystore.password() != null) p.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystore.password());
            }
        }
        if (truststore != null) {
            if (truststore.isPem()) {
                p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
                p.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, read(truststore.caPath()));
            } else {
                p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, truststore.type().toUpperCase());
                p.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore.path());
                if (truststore.password() != null) p.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststore.password());
            }
        }
    }

    private static String read(String path) {
        try {
            return Files.readString(Path.of(path));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read PEM file at " + path, e);
        }
    }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
    private static String orDefault(String s, String d) { return isBlank(s) ? d : s; }
}
