package se.afshin.yavari.kafka.smt;

import org.apache.kafka.common.config.ConfigException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds an {@link SSLContext} for a registry client from Kafka-style {@code ssl.*}
 * settings. Supports {@code PKCS12} and {@code JKS} stores, and {@code PEM} where the
 * keystore location is a certificate chain file plus a separate PKCS#8 private-key file
 * (RSA or EC), and the truststore location is a CA bundle. Uses only the JDK.
 */
public final class RegistryTls {

    private static final Pattern PEM_KEY = Pattern.compile(
            "-----BEGIN (?:(RSA|EC) )?PRIVATE KEY-----([^-]+)-----END (?:(?:RSA|EC) )?PRIVATE KEY-----");

    /** One side's {@code ssl.*} settings. Nulls mean "not configured". */
    public record Settings(String keystoreLocation, String keystorePassword, String keystoreType,
                           String keyLocation,
                           String truststoreLocation, String truststorePassword, String truststoreType) {

        boolean isEmpty() {
            return keystoreLocation == null && truststoreLocation == null;
        }
    }

    private RegistryTls() {}

    /** Returns an SSLContext for the settings, or {@code null} when neither a keystore nor
     *  a truststore is configured (the JDK default context applies). */
    public static SSLContext build(Settings s) {
        if (s == null || s.isEmpty()) return null;
        try {
            KeyManager[] kms = null;
            if (s.keystoreLocation() != null) {
                KeyStore ks = isPem(s.keystoreType())
                        ? pemKeyStore(s.keystoreLocation(), s.keyLocation())
                        : loadStore(s.keystoreLocation(), s.keystorePassword(), s.keystoreType());
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, isPem(s.keystoreType()) ? new char[0] : chars(s.keystorePassword()));
                kms = kmf.getKeyManagers();
            }
            TrustManager[] tms = null;
            if (s.truststoreLocation() != null) {
                KeyStore ts = isPem(s.truststoreType())
                        ? pemTrustStore(s.truststoreLocation())
                        : loadStore(s.truststoreLocation(), s.truststorePassword(), s.truststoreType());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                tms = tmf.getTrustManagers();
            }
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kms, tms, null);
            return ctx;
        } catch (ConfigException e) {
            throw e;
        } catch (IOException | GeneralSecurityException e) {
            throw new ConfigException("Failed to build registry TLS context: " + e.getMessage());
        }
    }

    private static boolean isPem(String type) {
        return "PEM".equalsIgnoreCase(type);
    }

    private static char[] chars(String password) {
        return password == null ? new char[0] : password.toCharArray();
    }

    private static KeyStore loadStore(String location, String password, String type)
            throws IOException, GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance(type == null ? "PKCS12" : type);
        try (InputStream in = Files.newInputStream(Path.of(location))) {
            ks.load(in, chars(password));
        }
        return ks;
    }

    private static KeyStore pemTrustStore(String location) throws IOException, GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        int i = 0;
        for (Certificate c : readCerts(location)) {
            ks.setCertificateEntry("ca-" + i++, c);
        }
        if (i == 0) throw new ConfigException("No certificates found in truststore PEM " + location);
        return ks;
    }

    private static KeyStore pemKeyStore(String certLocation, String keyLocation)
            throws IOException, GeneralSecurityException {
        if (keyLocation == null) {
            throw new ConfigException("ssl.keystore.type=PEM requires ssl.key.location (PKCS#8 private key)");
        }
        List<Certificate> chain = new ArrayList<>(readCerts(certLocation));
        if (chain.isEmpty()) throw new ConfigException("No certificates found in keystore PEM " + certLocation);
        PrivateKey key = readPkcs8Key(keyLocation);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("client", key, new char[0], chain.toArray(new Certificate[0]));
        return ks;
    }

    private static Collection<? extends Certificate> readCerts(String location)
            throws IOException, GeneralSecurityException {
        byte[] pem = Files.readAllBytes(Path.of(location));
        return CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem));
    }

    private static PrivateKey readPkcs8Key(String location) throws IOException, GeneralSecurityException {
        String pem = Files.readString(Path.of(location), StandardCharsets.UTF_8);
        Matcher m = PEM_KEY.matcher(pem);
        if (!m.find()) {
            throw new ConfigException("No PRIVATE KEY block found in " + location
                    + " (PKCS#8 'BEGIN PRIVATE KEY' expected)");
        }
        if (m.group(1) != null) {
            throw new ConfigException("Private key in " + location + " is PKCS#1/SEC1 ('BEGIN "
                    + m.group(1) + " PRIVATE KEY'); convert it to PKCS#8 with "
                    + "'openssl pkcs8 -topk8 -nocrypt'");
        }
        byte[] der = Base64.getMimeDecoder().decode(m.group(2).trim());
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        GeneralSecurityException last = null;
        for (String alg : new String[] {"RSA", "EC"}) {
            try {
                return KeyFactory.getInstance(alg).generatePrivate(spec);
            } catch (GeneralSecurityException e) {
                last = e;
            }
        }
        throw new ConfigException("Unsupported private key in " + location + ": " + last.getMessage());
    }
}
