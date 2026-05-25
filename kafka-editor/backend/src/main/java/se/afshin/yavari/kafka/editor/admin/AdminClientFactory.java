package se.afshin.yavari.kafka.editor.admin;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.quarkus.oidc.AccessTokenCredential;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import se.afshin.yavari.kafka.editor.auth.JwtCallbackHandler;

/**
 * Builds Kafka clients for the admin layer.
 *
 * <p><b>Two deployment modes:</b>
 * <ul>
 *   <li><b>Operator-deployed (production):</b> {@code kafka-editor.bootstrap-servers}
 *       is injected by {@code UIDeploymentBuilder}; the user's JWT is forwarded
 *       as SASL/OAUTHBEARER to the KafkaProxy, and a shared client cert is
 *       loaded from {@code kafka-editor.tls-dir} for transport mTLS. Per-request
 *       caching: each user gets their own clients, closed when the request ends.</li>
 *   <li><b>Standalone (upstream local dev):</b> {@code kafka-editor.bootstrap-servers}
 *       is unset, so the per-request {@link ConnectionConfig} bootstrap is used;
 *       PLAINTEXT, no token. Same per-request caching.</li>
 * </ul>
 *
 * <p>{@link KafkaConsumer} is <em>not</em> thread-safe: each {@link #newConsumer}
 * call returns a fresh instance and registers it for close at request end.
 */
@RequestScoped
public class AdminClientFactory {

    private static final Logger LOG = Logger.getLogger(AdminClientFactory.class);

    static final int TIMEOUT_MS = 15_000;
    private static final int FETCH_MAX_BYTES = 4 * 1024 * 1024;

    /** Operator injects this in production; empty means use the per-request connection. */
    @Inject
    @ConfigProperty(name = "kafka-editor.bootstrap-servers")
    Optional<String> injectedBootstrap;

    /** Transport security on the wire to Kafka. SASL_SSL in production. */
    @Inject
    @ConfigProperty(name = "kafka-editor.security-protocol", defaultValue = "PLAINTEXT")
    String securityProtocol;

    /**
     * Directory holding PEM files: {@code tls.crt}, {@code tls.key}, {@code ca.crt}.
     * Typically mounted from a Kubernetes Secret of type {@code kubernetes.io/tls}.
     * When empty/unset, no client cert is configured.
     */
    @Inject
    @ConfigProperty(name = "kafka-editor.tls-dir")
    Optional<String> tlsDir;

    /** Quarkus OIDC injects the access token of the current authenticated request.
     *  Wrapped in {@link Instance} so the bean still resolves in the %test profile
     *  where OIDC is disabled. */
    @Inject
    Instance<AccessTokenCredential> token;

    private final Map<String, Admin> adminClients = new HashMap<>();
    private final Map<String, KafkaProducer<byte[], byte[]>> producers = new HashMap<>();
    private final Map<String, KafkaConsumer<byte[], byte[]>> consumers = new HashMap<>();

    /** A cached admin client for the connection's bootstrap servers. */
    public Admin admin(ConnectionConfig connection) {
        return admin(connection.bootstrapServersOrDefault());
    }

    /** A cached admin client for the given bootstrap servers. */
    public Admin admin(String bootstrapServers) {
        String key = effectiveBootstrap(bootstrapServers);
        return adminClients.computeIfAbsent(key, k -> Admin.create(adminProps(k, injectedBootstrap.orElse(""), securityProtocol, tlsDir.orElse(""), currentToken())));
    }

    /** A cached, thread-safe byte-array producer. */
    public KafkaProducer<byte[], byte[]> producer(String bootstrapServers) {
        String key = effectiveBootstrap(bootstrapServers);
        return producers.computeIfAbsent(key, k -> new KafkaProducer<>(producerProps(k, injectedBootstrap.orElse(""), securityProtocol, tlsDir.orElse(""), currentToken())));
    }

    /**
     * A fresh byte-array consumer with a unique group id and no auto-commit.
     * Consumers are not thread-safe — the caller owns it; the factory tracks
     * the instance so it is closed at request end.
     */
    public KafkaConsumer<byte[], byte[]> newConsumer(String bootstrapServers) {
        String key = effectiveBootstrap(bootstrapServers);
        KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(key, injectedBootstrap.orElse(""), securityProtocol, tlsDir.orElse(""), currentToken()));
        consumers.put(UUID.randomUUID().toString(), c);
        return c;
    }

    /**
     * A quick reachability probe — {@code null} if the broker answered, an
     * error message otherwise.
     */
    public String probe(String bootstrapServers) {
        try {
            admin(bootstrapServers).listTopics().names()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return null;
        } catch (Exception e) {
            return "Could not reach the Kafka broker at " + effectiveBootstrap(bootstrapServers)
                    + ". Is it running? (docker compose up -d)";
        }
    }

    /** When operator injects a bootstrap, that overrides what the SPA sent. */
    private String effectiveBootstrap(String requested) {
        return injectedBootstrap
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .orElseGet(() -> normalize(requested));
    }

    /** Resolve the OIDC bearer token if OIDC is active, else null. */
    private String currentToken() {
        if (token == null || token.isUnsatisfied()) return null;
        try {
            AccessTokenCredential cred = token.get();
            return cred == null ? null : cred.getToken();
        } catch (Exception e) {
            // No active authenticated request — happens during the %test profile.
            return null;
        }
    }

    static Properties adminProps(String bootstrapServers) {
        return adminProps(bootstrapServers, "", "PLAINTEXT", "", null);
    }

    static Properties adminProps(String bootstrapServers, String injectedBootstrap,
                                 String securityProtocol, String tlsDir, String rawToken) {
        Properties props = baseProps(bootstrapServers, injectedBootstrap, securityProtocol, tlsDir, rawToken);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        // The total budget for an operation, retries included. Multi-step calls
        // (e.g. listConsumerGroups) need retries within this window, so we do
        // NOT pin retries to 0 — DEFAULT_API_TIMEOUT_MS bounds the wait.
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-editor-admin-" + UUID.randomUUID());
        return props;
    }

    static Properties consumerProps(String bootstrapServers, String injectedBootstrap,
                                    String securityProtocol, String tlsDir, String rawToken) {
        Properties props = baseProps(bootstrapServers, injectedBootstrap, securityProtocol, tlsDir, rawToken);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-editor-browse-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, FETCH_MAX_BYTES);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TIMEOUT_MS);
        return props;
    }

    static Properties producerProps(String bootstrapServers, String injectedBootstrap,
                                    String securityProtocol, String tlsDir, String rawToken) {
        Properties props = baseProps(bootstrapServers, injectedBootstrap, securityProtocol, tlsDir, rawToken);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 8_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        return props;
    }

    /** Shared bootstrap + security config. Visible for test. */
    static Properties baseProps(String bootstrapServers, String injectedBootstrap,
                                String securityProtocol, String tlsDir, String rawToken) {
        String effective = injectedBootstrap != null && !injectedBootstrap.isBlank()
                ? injectedBootstrap.trim()
                : normalize(bootstrapServers);

        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, effective);

        SecurityProtocol proto = SecurityProtocol.forName(
                securityProtocol == null || securityProtocol.isBlank() ? "PLAINTEXT" : securityProtocol);
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, proto.name);

        if (proto == SecurityProtocol.SASL_SSL || proto == SecurityProtocol.SSL) {
            Optional.ofNullable(tlsDir).filter(d -> !d.isBlank()).ifPresent(d -> {
                try {
                    java.nio.file.Path dir = java.nio.file.Path.of(d);
                    String cert = java.nio.file.Files.readString(dir.resolve("tls.crt"));
                    String key = java.nio.file.Files.readString(dir.resolve("tls.key"));
                    String ca = java.nio.file.Files.readString(dir.resolve("ca.crt"));
                    // Inline PEM keystore + truststore — no PKCS12 conversion needed.
                    p.put("ssl.keystore.type", "PEM");
                    p.put("ssl.keystore.certificate.chain", cert);
                    p.put("ssl.keystore.key", key);
                    p.put("ssl.truststore.type", "PEM");
                    p.put("ssl.truststore.certificates", ca);
                    // Hostnames in the proxy cert are SANs for the cluster.local /
                    // clusterset.local Service names; disable hostname verification
                    // since the dynamic Service IP won't match the cert SANs.
                    p.put("ssl.endpoint.identification.algorithm", "");
                } catch (java.io.IOException e) {
                    throw new IllegalStateException("Failed to read TLS PEM files from " + d, e);
                }
            });
        }

        if (proto == SecurityProtocol.SASL_SSL || proto == SecurityProtocol.SASL_PLAINTEXT) {
            if (rawToken == null || rawToken.isBlank()) {
                throw new IllegalStateException(
                        "SASL configured but no OIDC bearer token available — request is unauthenticated");
            }
            p.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            // Token is carried as a JAAS option; tag is a no-op marker that
            // makes each AdminClient's JAAS string unique (defeats kafka-clients'
            // login-subject cache).
            String tag = UUID.randomUUID().toString();
            p.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                            + "rawToken=\"" + rawToken + "\" "
                            + "tag=\"" + tag + "\";");
            p.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                    JwtCallbackHandler.class.getName());
        }

        return p;
    }

    static String normalize(String bootstrapServers) {
        return bootstrapServers == null || bootstrapServers.isBlank()
                ? "localhost:9092"
                : bootstrapServers.trim();
    }

    @PreDestroy
    void shutdown() {
        adminClients.values().forEach(admin -> {
            try { admin.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "Admin close failed"); }
        });
        producers.values().forEach(producer -> {
            try { producer.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "Producer close failed"); }
        });
        consumers.values().forEach(consumer -> {
            try { consumer.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "Consumer close failed"); }
        });
        adminClients.clear();
        producers.clear();
        consumers.clear();
    }
}
