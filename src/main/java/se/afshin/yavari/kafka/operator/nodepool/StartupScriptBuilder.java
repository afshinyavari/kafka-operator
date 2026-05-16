package se.afshin.yavari.kafka.operator.nodepool;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

@ApplicationScoped
public class StartupScriptBuilder {

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    public String build(KafkaNodePool pool, int clusterIndex, String namespace) {
        boolean isBroker = pool.getSpec().getRoles().contains(NodeRole.BROKER);
        String poolName = pool.getMetadata().getName();

        if (!isBroker) {
            return "#!/bin/bash\nset -euo pipefail\n"
                    + "/opt/kafka/bin/kafka-storage.sh format "
                    + "-t \"${KAFKA_CLUSTER_ID}\" "
                    + "-c /opt/kafka-config/server.properties.template "
                    + "--ignore-formatted\n"
                    + "exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka-config/server.properties.template\n";
        }

        String advertisedAddr = mcsEnabled
                ? poolName + "-headless." + namespace + ".svc.clusterset.local"
                : "${POD_NAME}." + poolName + "-headless." + namespace + ".svc.cluster.local";
        boolean hasRack = pool.getSpec().getRackTopologyKey() != null
                && !pool.getSpec().getRackTopologyKey().isBlank();
        String rackLine = hasRack
                ? "BROKER_RACK=$(cat /opt/kafka/init/rack.id 2>/dev/null || true)\n"
                : "";
        String rackSed = hasRack
                ? "    -e \"s|\\${BROKER_RACK}|${BROKER_RACK}|g\" \\\n"
                : "";

        return "#!/bin/bash\n"
                + "set -euo pipefail\n"
                + "ORDINAL=${POD_NAME##*-}\n"
                + "NODE_ID=$(( " + clusterIndex + " * " + KRaftConfigGenerator.BROKER_MULTIPLIER + " + ORDINAL ))\n"
                + "ADVERTISED_ADDR=" + advertisedAddr + ":9092\n"
                + rackLine
                + "sed -e \"s/\\${NODE_ID}/${NODE_ID}/g\" \\\n"
                + "    -e \"s|\\${ADVERTISED_ADDR}|${ADVERTISED_ADDR}|g\" \\\n"
                + rackSed
                + "    /opt/kafka-config/server.properties.template > /tmp/server.properties\n"
                + "/opt/kafka/bin/kafka-storage.sh format "
                + "-t \"${KAFKA_CLUSTER_ID}\" "
                + "-c /tmp/server.properties "
                + "--ignore-formatted\n"
                + "exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties\n";
    }
}
