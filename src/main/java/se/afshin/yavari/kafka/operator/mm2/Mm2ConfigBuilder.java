package se.afshin.yavari.kafka.operator.mm2;

import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.crd.Mm2FlowConfig;
import se.afshin.yavari.kafka.operator.crd.Mm2SchemaSyncConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Renders an MM2 properties file for the dedicated-mode driver
 * ({@code bin/connect-mirror-maker.sh}).
 *
 * <p>Conventions:
 * <ul>
 *   <li>Cluster aliases are {@code source} and {@code target}.
 *   <li>The replication flow is {@code source->target}.
 *   <li>Internal-topic names are suffixed with {@code flow.flowName} so two MM2 CRs
 *       can't collide.
 *   <li>TLS keystores are read from {@code /etc/mm2/pkcs12/{source,target}/} (written
 *       by the {@code pem-to-pkcs12} init containers).
 *   <li>SASL JAAS strings reference mounted Secret paths (no plaintext credentials in
 *       the properties).
 *   <li>Schema-sync SMT is appended when {@code spec.schemaSync.enabled=true} AND both
 *       ends have a schema registry URL.
 * </ul>
 */
@ApplicationScoped
public class Mm2ConfigBuilder {

    /** Where the per-side PKCS12 keystores land in the worker container. */
    public static final String PKCS12_BASE = "/etc/mm2/pkcs12";
    public static final String PKCS12_PASSWORD = "changeit";
    /** Where SASL secrets are mounted (each as {@code username}/{@code password} files). */
    public static final String SASL_BASE = "/etc/mm2/sasl";
    /** Schema registry HTTP auth Secrets (token or username/password). */
    public static final String REGISTRY_AUTH_BASE = "/etc/mm2/registry-auth";

    public String build(MirrorMaker2 cr, ResolvedEndpoint source, ResolvedEndpoint target) {
        MirrorMaker2Spec spec = cr.getSpec();
        Mm2FlowConfig flow = spec.getFlow() != null ? spec.getFlow() : new Mm2FlowConfig();
        String flowName = cr.resolvedFlowName();
        Map<String, String> p = new LinkedHashMap<>();

        // Cluster aliases
        p.put("clusters", "source,target");
        p.put("source.bootstrap.servers", source.bootstrap());
        p.put("target.bootstrap.servers", target.bootstrap());
        applySecurity(p, "source", source);
        applySecurity(p, "target", target);

        // Replication flow gating
        p.put("source->target.enabled", "true");
        p.put("source->target.topics", String.join(",", flow.getTopics()));
        if (flow.getTopicsExclude() != null && !flow.getTopicsExclude().isEmpty()) {
            p.put("source->target.topics.exclude", String.join(",", flow.getTopicsExclude()));
        }
        p.put("source->target.groups", String.join(",", flow.getGroups()));
        if (flow.getGroupsExclude() != null && !flow.getGroupsExclude().isEmpty()) {
            p.put("source->target.groups.exclude", String.join(",", flow.getGroupsExclude()));
        }
        p.put("source->target.replication.factor", String.valueOf(flow.getReplicationFactor()));
        p.put("source->target.sync.topic.configs.enabled", String.valueOf(flow.isSyncTopicConfigs()));
        p.put("source->target.sync.topic.acls.enabled", String.valueOf(flow.isSyncTopicAcls()));
        p.put("source->target.emit.heartbeats.enabled", String.valueOf(flow.isEmitHeartbeats()));
        p.put("source->target.tasks.max", String.valueOf(flow.getTasksMax()));

        // Internal topics — pin per-flow names so two MM2 CRs can coexist on the same target.
        p.put("offset.storage.topic", "mm2-offsets." + flowName);
        p.put("config.storage.topic", "mm2-configs." + flowName);
        p.put("status.storage.topic", "mm2-status." + flowName);
        p.put("offset.storage.replication.factor", String.valueOf(flow.getReplicationFactor()));
        p.put("config.storage.replication.factor", String.valueOf(flow.getReplicationFactor()));
        p.put("status.storage.replication.factor", String.valueOf(flow.getReplicationFactor()));

        // Schema-sync SMT — appended to the MirrorSourceConnector transforms chain.
        Mm2SchemaSyncConfig schemaSync = spec.getSchemaSync();
        if (schemaSync != null && schemaSync.isEnabled()
                && source.hasSchemaRegistry() && target.hasSchemaRegistry()) {
            String prefix = "source->target.";
            p.put(prefix + "transforms", "schemaSync");
            p.put(prefix + "transforms.schemaSync.type",
                    "se.afshin.yavari.kafka.smt.ApicurioSchemaTransferSmt");
            p.put(prefix + "transforms.schemaSync.source.url", source.schemaRegistryUrl());
            p.put(prefix + "transforms.schemaSync.target.url", target.schemaRegistryUrl());
            p.put(prefix + "transforms.schemaSync.cache.size", String.valueOf(schemaSync.getCacheSize()));
            p.put(prefix + "transforms.schemaSync.behavior.on.error", schemaSync.getBehaviorOnError().name());
            p.put(prefix + "transforms.schemaSync.apply.to", schemaSync.getApplyTo().name());
            p.put(prefix + "transforms.schemaSync.apply.to.topics",
                    String.join(",", schemaSync.getApplyToTopics()));
            // Schema-registry auth — OAuth2 client-credentials read from a mounted Secret dir.
            // The SMT does the token fetch/refresh; the operator only points it at the dir.
            if (source.schemaRegistryAuthSecretRef() != null) {
                p.put(prefix + "transforms.schemaSync.source.auth.oauth.dir",
                        REGISTRY_AUTH_BASE + "/source");
            }
            if (target.schemaRegistryAuthSecretRef() != null) {
                p.put(prefix + "transforms.schemaSync.target.auth.oauth.dir",
                        REGISTRY_AUTH_BASE + "/target");
            }
        }

        // Passthrough escape hatch — overrides everything above except the cluster aliases.
        if (flow.getAdditionalProperties() != null) {
            p.putAll(flow.getAdditionalProperties());
        }
        return p.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("\n")) + "\n";
    }

    private void applySecurity(Map<String, String> p, String alias, ResolvedEndpoint ep) {
        String securityProtocol;
        if (ep.hasTls() && ep.hasSasl()) {
            securityProtocol = "SASL_SSL";
        } else if (ep.hasTls()) {
            securityProtocol = "SSL";
        } else if (ep.hasSasl()) {
            securityProtocol = "SASL_PLAINTEXT";
        } else {
            return; // PLAINTEXT — leave protocol unset (Kafka default)
        }
        p.put(alias + ".security.protocol", securityProtocol);
        if (ep.hasTls()) {
            String base = PKCS12_BASE + "/" + alias;
            p.put(alias + ".ssl.keystore.type", "PKCS12");
            p.put(alias + ".ssl.keystore.location", base + "/keystore.p12");
            p.put(alias + ".ssl.keystore.password", PKCS12_PASSWORD);
            p.put(alias + ".ssl.key.password", PKCS12_PASSWORD);
            p.put(alias + ".ssl.truststore.type", "PKCS12");
            p.put(alias + ".ssl.truststore.location", base + "/truststore.p12");
            p.put(alias + ".ssl.truststore.password", PKCS12_PASSWORD);
            // A managed cluster's proxy advertises broker addresses (LB IPs / clusterset.local
            // names) that its server-cert SANs don't all cover. Disable hostname verification —
            // the cert chain is still validated against the truststore CA — matching every
            // other client that reaches brokers through the proxy.
            p.put(alias + ".ssl.endpoint.identification.algorithm", "");
        }
        if (ep.hasSasl()) {
            p.put(alias + ".sasl.mechanism", ep.sasl().mechanism());
            // JAAS string refers to mounted Secret files via Kafka's PEM loader is awkward,
            // so we inline the username/password from the Secret at deploy time via an init
            // script — but that requires a wrapper. v1 simpler path: rely on env-var
            // substitution at startup (handled in the launcher script in the MM2 image),
            // with placeholders here that the launcher resolves from /etc/mm2/sasl/{alias}.
            // For correctness on first ship, embed a JAAS template the launcher rewrites:
            p.put(alias + ".sasl.jaas.config",
                    "${file:" + SASL_BASE + "/" + alias + "/jaas.conf:" + alias + "}");
        }
    }
}
