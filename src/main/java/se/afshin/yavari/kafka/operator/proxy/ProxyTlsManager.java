package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.io.StringWriter;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Manages TLS secrets for proxy↔broker mTLS.
 * Generates and stores a self-signed CA + broker cert + proxy client cert.
 * Idempotent: no-ops if secrets already exist.
 */
@ApplicationScoped
public class ProxyTlsManager {

    private static final Logger LOG = Logger.getLogger(ProxyTlsManager.class);

    @Inject
    KubernetesClient client;

    public static String brokerSecretName(String proxyName) {
        return proxyName + "-tls-broker";
    }

    public static String proxySecretName(String proxyName) {
        return proxyName + "-tls-proxy";
    }

    /** Ensures broker and proxy TLS secrets exist; creates them if missing. */
    public void ensureSecrets(String proxyName, String poolName, String namespace) {
        String brokerSN = brokerSecretName(proxyName);
        String proxySN = proxySecretName(proxyName);
        if (client.secrets().inNamespace(namespace).withName(brokerSN).get() != null
                && client.secrets().inNamespace(namespace).withName(proxySN).get() != null) {
            return;
        }
        LOG.infof("Generating mTLS secrets for proxy %s (pool: %s)", proxyName, poolName);
        try {
            generateAndStore(proxyName, poolName, namespace);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate mTLS secrets for proxy " + proxyName, e);
        }
    }

    /** Deletes the TLS secrets created for this proxy. */
    public void deleteSecrets(String proxyName, String namespace) {
        client.secrets().inNamespace(namespace).withName(brokerSecretName(proxyName)).delete();
        client.secrets().inNamespace(namespace).withName(proxySecretName(proxyName)).delete();
    }

    private void generateAndStore(String proxyName, String poolName, String namespace) throws Exception {
        // CA key pair + self-signed cert
        KeyPair caKp = newKeyPair();
        X509Certificate caCert = buildCaCert(caKp);

        // Broker cert: include both cluster.local (bootstrap) and clusterset.local (Submariner/MCS)
        // SANs because Kroxylicious uses metadata addresses for node-specific upstream connections.
        List<String> brokerSans = List.of(
                poolName + "-headless." + namespace + ".svc.cluster.local",
                poolName + "-headless." + namespace + ".svc.clusterset.local");
        KeyPair brokerKp = newKeyPair();
        X509Certificate brokerCert = buildSignedCert(
                new X500Name("CN=" + poolName), brokerSans, brokerKp.getPublic(), caKp, caCert);

        // Proxy client cert: CN used as super.users principal; no SAN needed (client cert only)
        KeyPair proxyKp = newKeyPair();
        X509Certificate proxyCert = buildSignedCert(
                new X500Name("CN=" + proxyName), List.of(), proxyKp.getPublic(), caKp, caCert);

        String caText = toPem(caCert);

        applySecret(namespace, brokerSecretName(proxyName), proxyName, Map.of(
                "tls.crt", toPem(brokerCert),
                "tls.key", toPem(brokerKp.getPrivate()),
                "ca.crt", caText));

        applySecret(namespace, proxySecretName(proxyName), proxyName, Map.of(
                "tls.crt", toPem(proxyCert),
                "tls.key", toPem(proxyKp.getPrivate()),
                "ca.crt", caText));
    }

    private static KeyPair newKeyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private static X509Certificate buildCaCert(KeyPair kp) throws Exception {
        X500Name subject = new X500Name("CN=kafka-operator-ca");
        Date notBefore = new Date();
        Date notAfter = new Date(System.currentTimeMillis() + 10L * 365 * 24 * 60 * 60 * 1000);
        var builder = new JcaX509v3CertificateBuilder(
                subject, BigInteger.ONE, notBefore, notAfter, subject, kp.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign | KeyUsage.digitalSignature));
        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").build(kp.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
    }

    private static X509Certificate buildSignedCert(X500Name subject, List<String> sans,
            java.security.PublicKey publicKey, KeyPair caKp, X509Certificate caCert) throws Exception {
        Date notBefore = new Date();
        Date notAfter = new Date(System.currentTimeMillis() + 10L * 365 * 24 * 60 * 60 * 1000);
        X500Name issuer = new X500Name(caCert.getSubjectX500Principal().getName());
        var builder = new JcaX509v3CertificateBuilder(
                issuer, BigInteger.valueOf(System.currentTimeMillis()), notBefore, notAfter,
                subject, publicKey);
        builder.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true,
                new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
        if (!sans.isEmpty()) {
            GeneralName[] names = sans.stream()
                    .map(s -> new GeneralName(GeneralName.dNSName, s))
                    .toArray(GeneralName[]::new);
            builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(names));
        }
        ContentSigner signer = new JcaContentSignerBuilder("SHA256WithRSA").build(caKp.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
    }

    private static String toPem(Object obj) throws Exception {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter w = new JcaPEMWriter(sw)) {
            if (obj instanceof java.security.PrivateKey pk) {
                // Kroxylicious/Netty requires PKCS#8 (BEGIN PRIVATE KEY), not PKCS#1.
                // Java's PrivateKey.getEncoded() always returns PKCS#8 DER bytes.
                w.writeObject(new PemObject("PRIVATE KEY", pk.getEncoded()));
            } else {
                w.writeObject(obj);
            }
        }
        return sw.toString();
    }

    private void applySecret(String namespace, String name, String proxyName, Map<String, String> pemData) {
        Map<String, String> encoded = new java.util.LinkedHashMap<>();
        pemData.forEach((k, v) -> encoded.put(k, Base64.getEncoder().encodeToString(v.getBytes())));

        Secret secret = new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE,
                        "kafka.yavari.afshin.se/proxy", proxyName))
                .endMetadata()
                .withType("kubernetes.io/tls")
                .withData(encoded)
                .build();
        client.secrets().inNamespace(namespace).resource(secret).serverSideApply();
    }
}
