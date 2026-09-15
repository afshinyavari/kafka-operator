package se.afshin.yavari.kafka.smt;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;

/** Generates a self-signed server + client identity pair with {@code keytool} and derives
 *  PKCS12, JKS and PEM material from it, for the mTLS tests. */
final class TestCerts {
    static final String PASSWORD = "changeit";

    final Path serverP12;      // server key + cert
    final Path serverTrustP12; // trusts client cert
    final Path clientP12;      // client key + cert
    final Path clientJks;      // same client identity as JKS
    final Path clientTrustP12; // trusts server cert
    final Path clientTrustJks;
    final Path clientCertPem;  // client cert chain (PEM)
    final Path clientKeyPem;   // client PKCS#8 key (PEM)
    final Path serverCertPem;  // server CA/cert (PEM)

    TestCerts(Path dir) throws Exception {
        serverP12 = dir.resolve("server.p12");
        serverTrustP12 = dir.resolve("server-trust.p12");
        clientP12 = dir.resolve("client.p12");
        clientJks = dir.resolve("client.jks");
        clientTrustP12 = dir.resolve("client-trust.p12");
        clientTrustJks = dir.resolve("client-trust.jks");
        clientCertPem = dir.resolve("client.crt");
        clientKeyPem = dir.resolve("client.key");
        serverCertPem = dir.resolve("server.crt");

        genKeyPair(serverP12, "server", "CN=localhost", "-ext", "san=ip:127.0.0.1,dns:localhost");
        genKeyPair(clientP12, "client", "CN=mm2-schema-sync");
        Path serverCer = dir.resolve("server.cer");
        Path clientCer = dir.resolve("client.cer");
        exportCert(serverP12, "server", serverCer);
        exportCert(clientP12, "client", clientCer);
        importCert(serverTrustP12, "PKCS12", "client", clientCer);
        importCert(clientTrustP12, "PKCS12", "server", serverCer);
        importCert(clientTrustJks, "JKS", "server", serverCer);
        keytool("-importkeystore", "-srckeystore", clientP12.toString(), "-srcstoretype", "PKCS12",
                "-srcstorepass", PASSWORD, "-destkeystore", clientJks.toString(),
                "-deststoretype", "JKS", "-deststorepass", PASSWORD);

        // PEM: certificate from keytool, PKCS#8 key via the KeyStore API.
        Files.copy(serverCer, serverCertPem);
        Files.copy(clientCer, clientCertPem);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(clientP12.toFile())) {
            ks.load(in, PASSWORD.toCharArray());
        }
        PrivateKey key = (PrivateKey) ks.getKey("client", PASSWORD.toCharArray());
        Files.writeString(clientKeyPem, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8)).encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n");
    }

    Certificate serverCert() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(serverP12.toFile())) {
            ks.load(in, PASSWORD.toCharArray());
        }
        return ks.getCertificate("server");
    }

    private static void genKeyPair(Path store, String alias, String dname, String... extra) throws IOException, InterruptedException {
        String[] base = {"-genkeypair", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
                "-alias", alias, "-dname", dname, "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", PASSWORD, "-keypass", PASSWORD};
        String[] args = new String[base.length + extra.length];
        System.arraycopy(base, 0, args, 0, base.length);
        System.arraycopy(extra, 0, args, base.length, extra.length);
        keytool(args);
    }

    private static void exportCert(Path store, String alias, Path out) throws IOException, InterruptedException {
        keytool("-exportcert", "-rfc", "-alias", alias, "-keystore", store.toString(),
                "-storetype", "PKCS12", "-storepass", PASSWORD, "-file", out.toString());
    }

    private static void importCert(Path store, String type, String alias, Path cert) throws IOException, InterruptedException {
        keytool("-importcert", "-noprompt", "-alias", alias, "-file", cert.toString(),
                "-keystore", store.toString(), "-storetype", type, "-storepass", PASSWORD);
    }

    private static void keytool(String... args) throws IOException, InterruptedException {
        String[] cmd = new String[args.length + 1];
        cmd[0] = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) throw new IOException("keytool failed: " + String.join(" ", args) + "\n" + out);
    }
}
