package se.afshin.yavari.kafka.operator.config;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ServerPropertiesBuilder {

    @Inject
    KRaftConfigGenerator kraftConfig;

    /**
     * Builds the merged Kafka properties for a node pool.
     * Precedence (lowest → highest): cluster {@code spec.config} → pool {@code spec.config} →
     * operator-computed fields. Computed fields ({@code process.roles}, {@code node.id},
     * {@code listeners}, {@code advertised.listeners}, {@code controller.quorum.voters}, etc.)
     * always win and cannot be overridden via user config.
     *
     * <p>The returned map is a template: broker-specific values ({@code ${NODE_ID}},
     * {@code ${ADVERTISED_ADDR}}, per-listener addr vars) are placeholder strings that
     * {@code start.sh} resolves at pod startup via {@code sed}.
     *
     * @param cr                          parent KafkaCluster CR
     * @param poolSpec                    the KafkaNodePool spec
     * @param clusterIndex                0-based index of this cluster in spec.clusters
     * @param poolLocalIndex              0-based index of this node within the pool (for broker node ID)
     * @param quorumVoters                pre-computed controller.quorum.voters string
     * @param controllerAdvertisedAddress host:port for the KRaft CONTROLLER listener on this cluster
     * @param brokerAdvertisedAddress     startup-time placeholder (e.g. {@code "${ADVERTISED_ADDR}"})
     *                                    substituted by start.sh; ignored for controller-only nodes
     * @param proxyCn                     CN of the Kroxylicious proxy cert (null = no proxy mTLS)
     * @param poolName                    pool name, used as the broker client CN in super.users
     */
    public Map<String, String> buildProperties(
            KafkaCluster cr,
            KafkaNodePoolSpec poolSpec,
            int clusterIndex,
            int poolLocalIndex,
            String quorumVoters,
            String controllerAdvertisedAddress,
            String brokerAdvertisedAddress,
            String proxyCn,
            String poolName) {

        KafkaClusterSpec clusterSpec = cr.getSpec();
        List<NodeRole> roles = poolSpec.getRoles();
        boolean isController = roles.contains(NodeRole.CONTROLLER);
        boolean isBroker = roles.contains(NodeRole.BROKER);

        // Determine process.roles string
        String processRoles;
        if (isController && isBroker) {
            processRoles = "controller,broker";
        } else if (isController) {
            processRoles = "controller";
        } else {
            processRoles = "broker";
        }

        // Determine node.id
        int nodeId = isController
                ? kraftConfig.controllerNodeId(clusterIndex)
                : kraftConfig.brokerNodeId(clusterIndex, poolLocalIndex);

        // Build listeners, advertised.listeners, and protocol map.
        // advertised.listeners must NOT include CONTROLLER and must be omitted for controller-only nodes.
        StringBuilder listeners = new StringBuilder();
        StringBuilder advertisedListeners = new StringBuilder();
        StringBuilder protocolMap = new StringBuilder();

        boolean internalMtls = proxyCn != null;
        KafkaListenerTlsConfig ctrlTls = clusterSpec.getControllerTls();
        List<KafkaListenerSpec> extraListeners = clusterSpec.getListeners();
        boolean hasExtraListeners = extraListeners != null && !extraListeners.isEmpty();
        // Only applies when the user configured a dedicated internal TLS listener (not auto mTLS)
        boolean hasInternalTlsListener = !internalMtls && hasExtraListeners && extraListeners.stream()
                .anyMatch(l -> l.getTls() != null);

        if (isController) {
            // Use pod IP placeholder; sed-substituted at startup. Kafka 3.9 rejects 0.0.0.0
            // in both listeners and derived advertised.listeners.
            listeners.append("CONTROLLER://${MY_POD_IP}:9093");
        }
        if (isBroker) {
            if (listeners.length() > 0) {
                listeners.append(',');
            }
            if (internalMtls) {
                // Proxy mTLS: INTERNAL stays at 0.0.0.0 (inter-broker cross-pod), but is now SSL
                listeners.append("INTERNAL://0.0.0.0:9092");
                advertisedListeners.append("INTERNAL://").append(brokerAdvertisedAddress);
            } else if (hasInternalTlsListener) {
                // User-configured internal TLS listener handles inter-broker: INTERNAL is localhost-only
                listeners.append("INTERNAL://127.0.0.1:9092");
                advertisedListeners.append("INTERNAL://127.0.0.1:9092");
            } else {
                listeners.append("INTERNAL://0.0.0.0:9092");
                advertisedListeners.append("INTERNAL://").append(brokerAdvertisedAddress);
            }
        }
        // CONTROLLER must always be in the protocol map — brokers use it to talk to controllers
        protocolMap.append("CONTROLLER:").append(ctrlTls != null ? "SSL" : "PLAINTEXT");
        if (isBroker) {
            protocolMap.append(",INTERNAL:").append(internalMtls ? "SSL" : "PLAINTEXT");
            if (hasExtraListeners) {
                for (KafkaListenerSpec l : extraListeners) {
                    listeners.append(',').append(l.getName()).append("://0.0.0.0:").append(l.getPort());
                    advertisedListeners.append(advertisedListeners.length() > 0 ? "," : "")
                                       .append(l.getName()).append("://${").append(l.getName()).append("_ADDR}");
                    protocolMap.append(',').append(l.getName())
                               .append(l.getTls() != null ? ":SSL" : ":PLAINTEXT");
                }
            }
        }

        // Merged config: cluster defaults → pool overrides → computed fields
        Map<String, String> props = new LinkedHashMap<>();

        // Cluster-level config first
        if (clusterSpec.getConfig() != null) {
            props.putAll(clusterSpec.getConfig());
        }
        // Pool-level overrides
        if (poolSpec.getConfig() != null) {
            props.putAll(poolSpec.getConfig());
        }

        // Computed fields — always overwrite user config to ensure correctness
        props.put("process.roles",               processRoles);
        props.put("node.id",                     String.valueOf(nodeId));
        props.put("cluster.id",                  kraftConfig.clusterIdFrom(cr));
        props.put("controller.quorum.voters",    quorumVoters);
        props.put("listeners",                   listeners.toString());
        if (isBroker) {
            props.put("advertised.listeners", advertisedListeners.toString());
        }
        if (isController && !isBroker) {
            // Kafka 3.9+ rejects 0.0.0.0 as an advertised listener; set the controller's
            // actual pod IP, substituted at startup time via the MY_POD_IP env var.
            props.put("controller.advertised.listeners", "CONTROLLER://${MY_POD_IP}:9093");
        }
        props.put("listener.security.protocol.map", protocolMap.toString());
        props.put("controller.listener.names",   "CONTROLLER");
        props.put("log.dirs",                    "/var/lib/kafka/data");

        if (isBroker) {
            String interBrokerListener;
            if (internalMtls) {
                interBrokerListener = "INTERNAL";
            } else {
                interBrokerListener = extraListeners.stream()
                        .findFirst().map(KafkaListenerSpec::getName).orElse("INTERNAL");
            }
            props.put("inter.broker.listener.name", interBrokerListener);
            if (internalMtls) {
                // Mutual TLS on the INTERNAL listener — requires client cert from all peers.
                // Hostname verification is disabled because the internal cert uses CN-only (no SAN);
                // CA trust still ensures only operator-issued certs can connect.
                addSslProps(props, "INTERNAL", true);
                props.put("listener.name.internal.ssl.endpoint.identification.algorithm", "");
            }
            // Per-listener SSL properties for each user-configured TLS listener
            for (KafkaListenerSpec l : extraListeners) {
                if (l.getTls() != null) {
                    addSslProps(props, l.getName(), l.getTls().isMutualTls());
                }
            }
            if (poolSpec.getRackTopologyKey() != null && !poolSpec.getRackTopologyKey().isBlank()) {
                props.put("broker.rack", "${BROKER_RACK}");
            }

            // Safe replication factor defaults
            int totalBrokerCount = estimateTotalBrokers(clusterSpec, poolSpec);
            int rf = Math.min(3, totalBrokerCount);
            props.putIfAbsent("offsets.topic.replication.factor",                  String.valueOf(rf));
            props.putIfAbsent("transaction.state.log.replication.factor",          String.valueOf(rf));
            props.putIfAbsent("transaction.state.log.min.isr",                     "2");
            props.putIfAbsent("default.replication.factor",                        String.valueOf(rf));
        }

        // Controller TLS SSL properties — applied to all roles (brokers also connect to controllers)
        if (ctrlTls != null) {
            addSslProps(props, "CONTROLLER", ctrlTls.isMutualTls());
        }

        if (internalMtls) {
            // super.users + authorizer apply to BOTH controllers and brokers. In KRaft mode
            // ACL writes flow broker → active controller, and the controller validates and
            // persists them; both processes must therefore know about the StandardAuthorizer
            // and treat the operator's principal (kafka-proxy) as super-user.
            props.put("super.users", "User:CN=" + proxyCn + ";User:CN=" + poolName);
            // All existing clients (operator AdminClient, Kroxylicious upstream) connect as
            // kafka-proxy → super-user → unaffected. Non-super-user principals (e.g. Apicurio
            // kafkasql registry) require explicit ACLs created via KafkaAclManager.
            props.put("authorizer.class.name",            "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
            props.put("allow.everyone.if.no.acl.found",   "false");
        }

        props.putIfAbsent("num.network.threads",                 "3");
        props.putIfAbsent("num.io.threads",                      "8");
        props.putIfAbsent("log.retention.hours",                 "168");
        props.putIfAbsent("log.segment.bytes",                   "1073741824");
        props.putIfAbsent("log.retention.check.interval.ms",     "300000");
        props.putIfAbsent("num.partitions",                      "3");

        return props;
    }

    /** Serializes the property map to a newline-delimited key=value string. */
    public String toPropertiesString(Map<String, String> props) {
        StringWriter sw = new StringWriter();
        props.forEach((k, v) -> sw.write(k + "=" + v + "\n"));
        return sw.toString();
    }

    private void addSslProps(Map<String, String> props, String listenerName, boolean mutualTls) {
        String pfx = "listener.name." + listenerName.toLowerCase() + ".ssl.";
        props.put(pfx + "keystore.location",   "/tmp/tls/" + listenerName + "/keystore.p12");
        props.put(pfx + "keystore.type",       "PKCS12");
        props.put(pfx + "keystore.password",   "changeit");
        props.put(pfx + "key.password",        "changeit");
        props.put(pfx + "truststore.location", "/tmp/tls/" + listenerName + "/truststore.p12");
        props.put(pfx + "truststore.type",     "PKCS12");
        props.put(pfx + "truststore.password", "changeit");
        if (mutualTls) {
            props.put(pfx + "client.auth", "required");
        }
    }

    /**
     * Rough estimate of total broker count across all clusters for setting replication factors.
     * Uses poolSpec.replicas as the per-cluster broker count and spec.clusters.size() as cluster count.
     */
    private int estimateTotalBrokers(KafkaClusterSpec spec, KafkaNodePoolSpec poolSpec) {
        return spec.getClusters().size() * poolSpec.getReplicas();
    }
}
