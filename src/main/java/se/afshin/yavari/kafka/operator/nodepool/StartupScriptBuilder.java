package se.afshin.yavari.kafka.operator.nodepool;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

@ApplicationScoped
public class StartupScriptBuilder {

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    /**
     * Generates the {@code start.sh} script that runs as the pod's entrypoint.
     * <p>At boot the script: (1) derives {@code NODE_ID} from the pod ordinal,
     * (2) computes {@code ADVERTISED_ADDR} (MCS clusterset.local or per-pod FQDN),
     * (3) resolves per-listener address variables (internal: from {@code ADVERTISED_ADDR};
     * NodePort: from {@code HOST_IP} + {@code EXTERNAL_*_NODEPORT} env vars),
     * (4) sed-substitutes all placeholders into {@code server.properties.template},
     * (5) formats storage idempotently via {@code kafka-storage.sh},
     * (6) converts TLS certs to PKCS12 keystores if TLS listeners are configured,
     * (7) execs {@code kafka-server-start.sh}.
     */
    public String build(KafkaNodePool pool, int clusterIndex, String namespace,
                        boolean hasMetrics, List<KafkaListenerSpec> listeners,
                        KafkaListenerTlsConfig controllerTls, boolean internalMtls) {
        boolean isBroker = pool.getSpec().getRoles().contains(NodeRole.BROKER);
        String poolName = pool.getMetadata().getName();
        // Set javaagent inline in start.sh so kubectl exec commands don't inherit it
        String jmxExport = hasMetrics
                ? "export KAFKA_OPTS=\"${KAFKA_OPTS:+${KAFKA_OPTS} }"
                  + "-javaagent:/opt/jmx-exporter/jmx-exporter.jar=9101:/opt/jmx-exporter-config/jmx-config.yaml\"\n"
                : "";

        if (!isBroker) {
            String ctrlTlsBlock = controllerTls != null ? buildPkcs12Block("CONTROLLER", "/etc/kafka/tls") : "";
            return "#!/bin/bash\nset -euo pipefail\n"
                    + "sed -e \"s|\\${MY_POD_IP}|${MY_POD_IP}|g\" "
                    + "/opt/kafka-config/server.properties.template > /tmp/server.properties\n"
                    + "/opt/kafka/bin/kafka-storage.sh format "
                    + "-t \"${KAFKA_CLUSTER_ID}\" "
                    + "-c /tmp/server.properties "
                    + "--ignore-formatted\n"
                    + ctrlTlsBlock
                    + jmxExport
                    + "exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties\n";
        }

        String advertisedAddr = mcsEnabled
                ? poolName + "-headless." + namespace + ".svc.clusterset.local"
                : "${POD_NAME}." + poolName + "-headless." + namespace + ".svc.cluster.local";
        boolean hasRack = pool.getSpec().getRackTopologyKey() != null
                && !pool.getSpec().getRackTopologyKey().isBlank();
        // BROKER_RACK is injected as a pod env var by PodTemplateFactory — no file read needed
        String rackSed = hasRack
                ? "    -e \"s|\\${BROKER_RACK}|${BROKER_RACK}|g\" \\\n"
                : "";

        // Build per-TLS-listener address vars, PKCS12 conversions, and sed substitutions
        StringBuilder tlsAddrVars = new StringBuilder();
        StringBuilder tlsSed = new StringBuilder();
        StringBuilder tlsPkcs12 = new StringBuilder();
        boolean hasListeners = listeners != null && !listeners.isEmpty();
        if (hasListeners) {
            for (KafkaListenerSpec l : listeners) {
                String n = l.getName();
                if (l.getExternalAccess() == ExternalAccessType.NODEPORT) {
                    // External NodePort: advertise the node IP (HOST_IP env var) + assigned nodePort
                    tlsAddrVars.append(n).append("_ADDR=\"${HOST_IP}:${EXTERNAL_")
                               .append(n).append("_NODEPORT}\"\n");
                } else {
                    // Internal listener: derive address from base hostname + listener port
                    tlsAddrVars.append(n).append("_ADDR=\"${ADVERTISED_ADDR%:*}:")
                               .append(l.getPort()).append("\"\n");
                }
                tlsSed.append("    -e \"s|\\${").append(n).append("_ADDR}|${")
                      .append(n).append("_ADDR}|g\" \\\n");
                if (l.getTls() != null) {
                    tlsPkcs12.append(buildPkcs12Block(n, "/etc/kafka/tls"));
                }
            }
        }
        // Proxy mTLS: broker cert is mounted at a separate path to avoid collision
        if (internalMtls) {
            tlsPkcs12.append(buildPkcs12Block("INTERNAL", "/etc/kafka/broker-mtls"));
        }
        // Broker also needs PKCS12 for CONTROLLER channel when controller TLS is enabled
        if (controllerTls != null) {
            tlsPkcs12.append(buildPkcs12Block("CONTROLLER", "/etc/kafka/tls"));
        }

        return "#!/bin/bash\n"
                + "set -euo pipefail\n"
                + "ORDINAL=${POD_NAME##*-}\n"
                + "NODE_ID=$(( " + clusterIndex + " * " + KRaftConfigGenerator.BROKER_MULTIPLIER + " + ORDINAL ))\n"
                + "ADVERTISED_ADDR=" + advertisedAddr + ":9092\n"
                + tlsAddrVars
                + "sed -e \"s/\\${NODE_ID}/${NODE_ID}/g\" \\\n"
                + "    -e \"s|\\${ADVERTISED_ADDR}|${ADVERTISED_ADDR}|g\" \\\n"
                + rackSed
                + tlsSed
                + "    /opt/kafka-config/server.properties.template > /tmp/server.properties\n"
                + "/opt/kafka/bin/kafka-storage.sh format "
                + "-t \"${KAFKA_CLUSTER_ID}\" "
                + "-c /tmp/server.properties "
                + "--ignore-formatted\n"
                + tlsPkcs12
                + jmxExport
                + "exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties\n";
    }

    private String buildPkcs12Block(String listenerName, String certBasePath) {
        return "mkdir -p /tmp/tls/" + listenerName + "\n"
                // Keystore: combine private key + cert into PKCS12
                + "openssl pkcs12 -export"
                + " -inkey " + certBasePath + "/tls.key"
                + " -in " + certBasePath + "/tls.crt"
                + " -out /tmp/tls/" + listenerName + "/keystore.p12"
                + " -passout pass:changeit 2>/dev/null\n"
                // Truststore: keytool creates proper Java-trusted cert entries from CA cert
                + "keytool -importcert -noprompt -trustcacerts"
                + " -alias ca -file " + certBasePath + "/ca.crt"
                + " -keystore /tmp/tls/" + listenerName + "/truststore.p12"
                + " -storetype PKCS12 -storepass changeit 2>/dev/null\n";
    }
}
