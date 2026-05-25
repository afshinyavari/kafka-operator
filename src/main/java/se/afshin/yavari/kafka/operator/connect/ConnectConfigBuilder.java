package se.afshin.yavari.kafka.operator.connect;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectWorkerConfig;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders the {@code connect-distributed.properties} file the Connect workers boot with.
 *
 * <p>Conventions:
 * <ul>
 *   <li>{@code group.id} = {@link KafkaConnect#resolvedGroupId()}.
 *   <li>Internal topic names are suffixed with {@code metadata.name} so two KafkaConnect
 *       CRs on one Kafka cluster don't collide.
 *   <li>TLS keystores are read from {@code /etc/connect/pkcs12/} (written by the
 *       {@code pem-to-pkcs12} init container).
 *   <li>REST listener binds to all interfaces on {@link KafkaConnectSpec#getRestPort()}.
 * </ul>
 */
@ApplicationScoped
public class ConnectConfigBuilder {

    public static final String PKCS12_BASE = "/etc/connect/pkcs12";
    public static final String PKCS12_PASSWORD = "changeit";
    public static final String SASL_BASE = "/etc/connect/sasl";

    public String build(KafkaConnect cr, ResolvedKafkaEndpoint endpoint,
                        ResolvedPluginSources plugins) {
        KafkaConnectSpec spec = cr.getSpec();
        KafkaConnectWorkerConfig worker = spec.getWorker() != null
                ? spec.getWorker() : new KafkaConnectWorkerConfig();
        String name = cr.getMetadata().getName();
        Map<String, String> p = new LinkedHashMap<>();

        p.put("bootstrap.servers", endpoint.bootstrap());
        p.put("group.id", cr.resolvedGroupId());

        // Internal topic names — per-CR so two KafkaConnect CRs can share one Kafka cluster.
        p.put("config.storage.topic", "connect-configs." + name);
        p.put("offset.storage.topic", "connect-offsets." + name);
        p.put("status.storage.topic", "connect-status." + name);
        int rf = worker.getInternalReplicationFactor();
        p.put("config.storage.replication.factor", String.valueOf(rf));
        p.put("offset.storage.replication.factor", String.valueOf(rf));
        p.put("status.storage.replication.factor", String.valueOf(rf));
        p.put("offset.storage.partitions", "25");
        p.put("status.storage.partitions", "5");

        // Converters
        p.put("key.converter", worker.getKeyConverter());
        p.put("value.converter", worker.getValueConverter());
        p.put("key.converter.schemas.enable", String.valueOf(worker.isKeyConverterSchemasEnable()));
        p.put("value.converter.schemas.enable", String.valueOf(worker.isValueConverterSchemasEnable()));

        // Plugin path — operator-shipped baked dir always present; PVC / ConfigMap / Secret
        // dirs appended by ConnectPluginResolver.
        p.put("plugin.path", plugins.pluginPath());

        // REST listener — binds to all interfaces. Connect auto-discovers per-worker REST
        // URLs through the group coordinator, so we don't need to advertise per-pod names.
        p.put("listeners", "http://0.0.0.0:" + spec.getRestPort());

        // Producer / consumer / admin security inherited from the endpoint.
        applySecurity(p, endpoint);

        // Free-form passthrough — last so it wins over everything except the cluster
        // identity (bootstrap, internal topic names, group.id) which the operator owns.
        Map<String, String> extras = worker.getAdditionalProperties();
        if (extras != null) {
            for (var e : extras.entrySet()) {
                String key = e.getKey();
                if (key.equals("bootstrap.servers") || key.equals("group.id")
                        || key.startsWith("config.storage.topic")
                        || key.startsWith("offset.storage.topic")
                        || key.startsWith("status.storage.topic")) {
                    continue;
                }
                p.put(key, e.getValue());
            }
        }

        return p.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n")) + "\n";
    }

    private void applySecurity(Map<String, String> p, ResolvedKafkaEndpoint ep) {
        String securityProtocol;
        if (ep.hasTls() && ep.hasSasl()) {
            securityProtocol = "SASL_SSL";
        } else if (ep.hasTls()) {
            securityProtocol = "SSL";
        } else if (ep.hasSasl()) {
            securityProtocol = "SASL_PLAINTEXT";
        } else {
            return;
        }
        // Connect applies security.protocol at three levels: top (admin client),
        // producer.*, consumer.*. Set them all so internal topic management, the
        // producer side of source connectors, and the consumer side of sink connectors
        // all reach the same cluster the same way.
        for (String prefix : new String[]{"", "producer.", "consumer.", "admin."}) {
            p.put(prefix + "security.protocol", securityProtocol);
            if (ep.hasTls()) {
                p.put(prefix + "ssl.keystore.type", "PKCS12");
                p.put(prefix + "ssl.keystore.location", PKCS12_BASE + "/keystore.p12");
                p.put(prefix + "ssl.keystore.password", PKCS12_PASSWORD);
                p.put(prefix + "ssl.key.password", PKCS12_PASSWORD);
                p.put(prefix + "ssl.truststore.type", "PKCS12");
                p.put(prefix + "ssl.truststore.location", PKCS12_BASE + "/truststore.p12");
                p.put(prefix + "ssl.truststore.password", PKCS12_PASSWORD);
                // Proxy advertises broker addresses outside its cert SANs — chain validates
                // against the truststore CA, hostname check disabled. Matches MM2 + AdminClient.
                p.put(prefix + "ssl.endpoint.identification.algorithm", "");
            }
            if (ep.hasSasl()) {
                p.put(prefix + "sasl.mechanism", ep.sasl().mechanism());
                p.put(prefix + "sasl.jaas.config",
                        "${file:" + SASL_BASE + "/jaas.conf:jaas}");
            }
        }
    }
}
