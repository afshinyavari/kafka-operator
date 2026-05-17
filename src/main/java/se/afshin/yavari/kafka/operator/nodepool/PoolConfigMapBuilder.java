package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.config.ServerPropertiesBuilder;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
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
        String brokerAddr = isBroker
                ? pool.getMetadata().getName() + "-0." + pool.getMetadata().getName()
                  + "-headless." + namespace + ".svc.cluster.local:9092"
                : null;

        Map<String, String> props = propsBuilder.buildProperties(
                cluster, pool.getSpec(), clusterIndex, 0, quorumVoters, controllerAddr, brokerAddr);

        if (isBroker) {
            props.put("node.id", "${NODE_ID}");
            props.put("advertised.listeners", "PLAINTEXT://${ADVERTISED_ADDR}");
        }

        String content = propsBuilder.toPropertiesString(props);
        MetricsConfig metrics = cluster.getSpec().getMetricsConfig();
        boolean hasMetrics = metrics != null && metrics.getConfigMapRef() != null;
        String startScript = scriptBuilder.build(pool, clusterIndex, namespace, hasMetrics);

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
