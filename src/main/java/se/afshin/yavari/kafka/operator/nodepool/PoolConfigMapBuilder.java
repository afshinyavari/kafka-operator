package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.config.ServerPropertiesBuilder;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.MetricsConfig;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class PoolConfigMapBuilder {

    @Inject
    ServerPropertiesBuilder propsBuilder;

    @Inject
    StartupScriptBuilder scriptBuilder;

    public ConfigMap build(KafkaNodePool pool, KafkaCluster cluster, String namespace,
                           int clusterIndex, String quorumVoters, String controllerAddr) {
        boolean isBroker = pool.getSpec().getRoles().contains(NodeRole.BROKER);
        List<KafkaListenerSpec> listeners = cluster.getSpec().getListeners();
        KafkaListenerTlsConfig controllerTls = cluster.getSpec().getControllerTls();
        boolean hasExtraListeners = listeners != null && !listeners.isEmpty();

        Map<String, String> props = propsBuilder.buildProperties(
                cluster, pool.getSpec(), clusterIndex, 0, quorumVoters, controllerAddr, null);

        if (isBroker) {
            props.put("node.id", "${NODE_ID}");
            if (!hasExtraListeners) {
                // No TLS listeners: INTERNAL is the only advertised listener, substitute at startup
                props.put("advertised.listeners", "INTERNAL://${ADVERTISED_ADDR}");
            }
            // When hasExtraListeners: buildProperties already set ${NAME_ADDR} template vars
        }

        String content = propsBuilder.toPropertiesString(props);
        MetricsConfig metrics = cluster.getSpec().getMetricsConfig();
        boolean hasMetrics = metrics != null && metrics.getConfigMapRef() != null;
        String startScript = scriptBuilder.build(pool, clusterIndex, namespace, hasMetrics, listeners, controllerTls);

        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(pool.getMetadata().getName() + "-config")
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    cluster.getMetadata().getName(),
                        KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(List.of(new OwnerReferenceBuilder()
                            .withApiVersion(pool.getApiVersion())
                            .withKind(pool.getKind())
                            .withName(pool.getMetadata().getName())
                            .withUid(pool.getMetadata().getUid())
                            .withController(true)
                            .withBlockOwnerDeletion(true)
                            .build()))
                .endMetadata()
                .addToData("server.properties.template", content)
                .addToData("start.sh", startScript)
                .build();
    }
}
