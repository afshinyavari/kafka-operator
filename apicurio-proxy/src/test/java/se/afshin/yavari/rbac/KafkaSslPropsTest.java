package se.afshin.yavari.rbac;

import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSslPropsTest {

    @Test
    void pkcs12StoresMapToLocationPasswordType() {
        Properties p = new Properties();
        KafkaSslProps.apply(p,
                new KafkaSslProps.Store("PKCS12", "/k/user.p12", "kpw", null, null, null),
                new KafkaSslProps.Store("PKCS12", "/t/ca.p12", "tpw", null, null, null));
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/k/user.p12")
                .containsEntry(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kpw")
                .containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/t/ca.p12")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "tpw")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
    }

    @Test
    void jksTypeIsPassedThrough() {
        Properties p = new Properties();
        KafkaSslProps.apply(p, new KafkaSslProps.Store("JKS", "/k/user.jks", "kpw", null, null, null), null);
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
                .doesNotContainKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    }

    @Test
    void pemStoresAreInlinedAsPemSourceMode(@TempDir Path dir) throws Exception {
        Path cert = Files.writeString(dir.resolve("tls.crt"), "CERT");
        Path key = Files.writeString(dir.resolve("tls.key"), "KEY");
        Path ca = Files.writeString(dir.resolve("ca.crt"), "CA");
        Properties p = new Properties();
        KafkaSslProps.apply(p,
                new KafkaSslProps.Store("PEM", null, null, cert.toString(), key.toString(), null),
                new KafkaSslProps.Store("PEM", null, null, null, null, ca.toString()));
        assertThat(p).containsEntry(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM")
                .containsEntry(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, "CERT")
                .containsEntry(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, "KEY")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, "CA");
    }

    @Test
    void fromEnvReadsPrefixedVariables() {
        Map<String, String> env = Map.of(
                "X_KEYSTORE", "/k.p12", "X_KEYSTORE_PASSWORD", "pw",
                "X_TRUSTSTORE", "/t.jks", "X_TRUSTSTORE_PASSWORD", "tpw", "X_TRUSTSTORE_TYPE", "JKS");
        KafkaSslProps.Store ks = KafkaSslProps.fromEnv(env::get, "X");
        KafkaSslProps.Store ts = KafkaSslProps.trustFromEnv(env::get, "X");
        assertThat(ks.type()).isEqualTo("PKCS12");
        assertThat(ks.path()).isEqualTo("/k.p12");
        assertThat(ts.type()).isEqualTo("JKS");
        assertThat(ts.password()).isEqualTo("tpw");
    }

    @Test
    void fromEnvPemFilesWithoutStoreBecomePemStore() {
        Map<String, String> env = Map.of("X_CERT", "/c.crt", "X_KEY", "/k.key", "X_CA", "/ca.crt");
        KafkaSslProps.Store ks = KafkaSslProps.fromEnv(env::get, "X");
        KafkaSslProps.Store ts = KafkaSslProps.trustFromEnv(env::get, "X");
        assertThat(ks.isPem()).isTrue();
        assertThat(ks.certPath()).isEqualTo("/c.crt");
        assertThat(ts.isPem()).isTrue();
        assertThat(ts.caPath()).isEqualTo("/ca.crt");
    }

    @Test
    void emptyStoreIsNullAndApplyIgnoresIt() {
        assertThat(KafkaSslProps.fromEnv(k -> null, "X")).isNull();
        assertThat(KafkaSslProps.trustFromEnv(k -> null, "X")).isNull();
        Properties p = new Properties();
        KafkaSslProps.apply(p, null, null);
        assertThat(p).isEmpty();
    }
}
