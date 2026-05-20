package se.afshin.yavari.kafka.operator.topic;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Base64;
import java.util.Map;
import java.util.Properties;

/**
 * Reads a cert-manager-style TLS Secret (keys: {@code tls.crt}, {@code tls.key}, {@code ca.crt}) and
 * produces Kafka client SSL properties using the PEM source mode that
 * {@code kafka-clients} has supported since 2.7. No keystore conversion to PKCS12/JKS
 * is required — the cert/key/CA strings are passed in directly.
 *
 * <p>The private key in {@code tls.key} must be in PKCS#8 PEM format (the format
 * {@code openssl pkcs8 -topk8} or cert-manager already produces).
 */
@ApplicationScoped
public class AdminClientTlsLoader {

    @Inject
    KubernetesClient client;

    /**
     * @throws TlsSecretNotFoundException if the Secret is missing or malformed
     */
    public Properties loadAsAdminClientSslProps(String namespace, String secretName) {
        Secret secret = client.secrets().inNamespace(namespace).withName(secretName).get();
        if (secret == null || secret.getData() == null) {
            throw new TlsSecretNotFoundException("TLS secret " + namespace + "/" + secretName
                    + " not found — required when KafkaCluster.spec.proxyMtls.enabled=true");
        }
        String tlsCrt = decodePemKey(secret, "tls.crt", namespace, secretName);
        String tlsKey = decodePemKey(secret, "tls.key", namespace, secretName);
        String caCrt  = decodePemKey(secret, "ca.crt",  namespace, secretName);

        Properties p = new Properties();
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        p.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        p.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, tlsCrt);
        p.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, new Password(tlsKey));
        p.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        p.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, caCrt);
        return p;
    }

    private static String decodePemKey(Secret secret, String key, String ns, String name) {
        Map<String, String> data = secret.getData();
        String b64 = data.get(key);
        if (b64 == null || b64.isBlank()) {
            throw new TlsSecretNotFoundException("Secret " + ns + "/" + name
                    + " is missing required PEM key '" + key + "' (expected cert-manager convention: tls.crt + tls.key + ca.crt)");
        }
        return new String(Base64.getDecoder().decode(b64));
    }

    public static class TlsSecretNotFoundException extends RuntimeException {
        public TlsSecretNotFoundException(String message) { super(message); }
    }
}
