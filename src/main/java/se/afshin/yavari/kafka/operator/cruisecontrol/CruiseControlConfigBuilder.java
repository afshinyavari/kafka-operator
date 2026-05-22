package se.afshin.yavari.kafka.operator.cruisecontrol;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.CruiseControlApiSecurity;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Renders the {@code cruisecontrol.properties} file for the Cruise Control deployment.
 *
 * <p>Precedence: user {@code spec.cruiseControl.config} entries are applied first, then
 * operator-computed keys overwrite them — the operator owns connectivity and file paths,
 * the user owns tuning. Cruise Control supplies code defaults for everything not emitted
 * here, so the generated file is intentionally lean.
 */
@ApplicationScoped
public class CruiseControlConfigBuilder {

    /** Directory the ConfigMap (properties + capacity) is mounted into. */
    public static final String CONFIG_DIR = "/etc/cruise-control";
    /** Directory the optional basic-auth Secret is mounted into (kept out of CONFIG_DIR
     *  so a Secret volume doesn't nest inside the ConfigMap volume). */
    public static final String AUTH_DIR = "/etc/cruise-control-auth";
    /** Directory the PKCS12 keystore/truststore are materialised into when broker mTLS is on.
     *  Matches the broker keystore path convention from {@code ServerPropertiesBuilder}. */
    public static final String TLS_DIR = "/tmp/tls/INTERNAL";

    static final String KEYSTORE_PASSWORD = "changeit";

    /**
     * @param spec             the Cruise Control sub-spec
     * @param bootstrapServers broker bootstrap address (INTERNAL listener)
     * @param mtls             true when the broker INTERNAL listener is mTLS
     */
    public String build(KafkaClusterCruiseControlSpec spec, String bootstrapServers, boolean mtls) {
        Map<String, String> props = new LinkedHashMap<>();

        // User overrides first — operator-computed keys below win.
        if (spec.getConfig() != null) {
            props.putAll(spec.getConfig());
        }

        // Connectivity + file paths (operator-owned).
        props.put("bootstrap.servers", bootstrapServers);
        // KRaft: detect broker failures via the AdminClient, no ZooKeeper.
        props.put("kafka.broker.failure.detection.enable", "true");
        props.put("webserver.http.port", String.valueOf(CruiseControlOrchestrator.REST_PORT));
        props.put("webserver.http.address", "0.0.0.0");
        props.put("capacity.config.file", CONFIG_DIR + "/capacity.json");
        props.put("metric.sampler.class",
                "com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler");
        props.put("sample.store.class",
                "com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore");
        // KafkaSampleStore requires explicit sample-topic names — Cruise Control
        // auto-creates the topics on startup. The replication factor is overridable
        // via spec.cruiseControl.config for clusters with fewer than 2 brokers.
        props.put("partition.metric.sample.store.topic", "__KafkaCruiseControlPartitionMetricSamples");
        props.put("broker.metric.sample.store.topic", "__KafkaCruiseControlModelTrainingSamples");
        props.putIfAbsent("sample.store.topic.replication.factor", "2");

        // Optional goal list. Cruise Control requires default.goals ⊆ goals — when the
        // user customises goals they own that consistency; unset → Cruise Control defaults.
        if (spec.getGoals() != null && !spec.getGoals().isEmpty()) {
            String goals = String.join(",", spec.getGoals());
            props.put("goals", goals);
            props.put("default.goals", goals);
        }

        // mTLS: Cruise Control's embedded AdminClient/consumer/producer connect over SSL,
        // reusing the PKCS12 keystore materialised by the PEM→PKCS12 init container.
        if (mtls) {
            props.put("security.protocol", "SSL");
            props.put("ssl.keystore.type", "PKCS12");
            props.put("ssl.keystore.location", TLS_DIR + "/keystore.p12");
            props.put("ssl.keystore.password", KEYSTORE_PASSWORD);
            props.put("ssl.key.password", KEYSTORE_PASSWORD);
            props.put("ssl.truststore.type", "PKCS12");
            props.put("ssl.truststore.location", TLS_DIR + "/truststore.p12");
            props.put("ssl.truststore.password", KEYSTORE_PASSWORD);
            // Internal broker certs are CN-only (no SAN) — disable hostname verification.
            props.put("ssl.endpoint.identification.algorithm", "");
        }

        CruiseControlApiSecurity api = spec.getApiSecurity();
        if (api != null && api.isEnabled()) {
            props.put("webserver.security.enable", "true");
            props.put("webserver.auth.credentials.file", AUTH_DIR + "/auth-credentials.properties");
            props.put("webserver.security.provider",
                    "com.linkedin.kafka.cruisecontrol.servlet.security.BasicSecurityProvider");
        }

        StringBuilder sb = new StringBuilder();
        props.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        return sb.toString();
    }
}
