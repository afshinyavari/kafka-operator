package se.afshin.yavari.kafka.ui.kafka;

import io.quarkus.oidc.AccessTokenCredential;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Builds {@link AdminClient} / {@link Consumer} instances for the logged-in
 * user, scoped to one HTTP request.
 *
 * <p>Each client carries the user's JWT inline in {@code sasl.jaas.config} (via
 * the {@code rawToken} option), so kafka-clients caches a separate login
 * subject per user. The JAAS config text is also salted with a per-request
 * UUID to defeat any accidental subject collision under concurrency.
 *
 * <p>All clients are closed in {@link #close()} when the request scope ends,
 * to avoid leaking connections.
 */
@RequestScoped
public class KafkaClientProvider {

    private static final Logger LOG = Logger.getLogger(KafkaClientProvider.class);

    @Inject
    ClusterRegistry registry;

    /** Quarkus OIDC injects the access token of the current authenticated request. */
    @Inject
    AccessTokenCredential token;

    @ConfigProperty(name = "kafka-ui.kafka.security-protocol", defaultValue = "SASL_SSL")
    String securityProtocol;

    /** Directory holding PEM files: {@code tls.crt}, {@code tls.key}, {@code ca.crt}.
     *  Typically mounted from a Kubernetes Secret of type {@code kubernetes.io/tls}.
     *  When empty / unset, no client cert is configured. */
    @ConfigProperty(name = "kafka-ui.kafka.ssl.tls-dir")
    java.util.Optional<String> tlsDir;

    private final Map<String, AdminClient> admins = new HashMap<>();
    private final Map<String, Consumer<byte[], byte[]>> consumers = new HashMap<>();
    private final Map<String, Producer<byte[], byte[]>> producers = new HashMap<>();

    public AdminClient admin(String clusterId) {
        return admins.computeIfAbsent(clusterId, id -> AdminClient.create(adminProps(coordinates(id))));
    }

    public Consumer<byte[], byte[]> consumer(String clusterId) {
        // Distinct group.id suffix per call so the same request can hold multiple
        // independent consumers (e.g. one per partition).
        String key = clusterId + "::" + UUID.randomUUID();
        Consumer<byte[], byte[]> c = new KafkaConsumer<>(consumerProps(coordinates(clusterId)));
        consumers.put(key, c);
        return c;
    }

    public Producer<byte[], byte[]> producer(String clusterId) {
        return producers.computeIfAbsent(clusterId, id -> new KafkaProducer<>(producerProps(coordinates(id))));
    }

    private ClusterCoordinates coordinates(String clusterId) {
        return registry.byId(clusterId)
                .orElseThrow(() -> new WebApplicationException(
                        "Unknown KafkaCluster: " + clusterId, Response.Status.NOT_FOUND));
    }

    private Properties adminProps(ClusterCoordinates c) {
        Properties p = baseProps(c.bootstrapUrl());
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-ui-admin-" + UUID.randomUUID());
        p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
        return p;
    }

    private Properties consumerProps(ClusterCoordinates c) {
        Properties p = baseProps(c.bootstrapUrl());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-ui-" + token.getToken().hashCode() + "-" + UUID.randomUUID());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        p.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "4194304");
        return p;
    }

    private Properties producerProps(ClusterCoordinates c) {
        Properties p = baseProps(c.bootstrapUrl());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-ui-producer-" + UUID.randomUUID());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000");
        return p;
    }

    private Properties baseProps(String bootstrap) {
        Properties p = new Properties();
        p.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        SecurityProtocol proto = SecurityProtocol.forName(securityProtocol);
        p.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, proto.name);

        if (proto == SecurityProtocol.SASL_SSL || proto == SecurityProtocol.SSL) {
            tlsDir.filter(d -> !d.isBlank()).ifPresent(d -> {
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
            p.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            // Token is carried as a JAAS option; tag is a no-op marker that
            // makes each AdminClient's JAAS string unique (defeats kafka-clients'
            // login-subject cache).
            String tag = UUID.randomUUID().toString();
            p.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                            + "rawToken=\"" + token.getToken() + "\" "
                            + "tag=\"" + tag + "\";");
            p.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
                    JwtCallbackHandler.class.getName());
        }

        return p;
    }

    @PreDestroy
    void close() {
        admins.forEach((id, ac) -> {
            try { ac.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "AdminClient close failed for %s", id); }
        });
        consumers.forEach((id, co) -> {
            try { co.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "Consumer close failed for %s", id); }
        });
        producers.forEach((id, pr) -> {
            try { pr.close(Duration.ofSeconds(2)); }
            catch (Exception e) { LOG.debugf(e, "Producer close failed for %s", id); }
        });
    }
}
