package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ExternalAccessServiceBuilder {

    private static final Logger LOG = Logger.getLogger(ExternalAccessServiceBuilder.class);

    /**
     * Idempotently creates or updates one NodePort {@code Service} per broker ordinal per
     * external listener. Service name: {@code {pool}-{ordinal}-{listener}-ext}.
     * NodePort assignment: {@code listener.nodePortBase + ordinal} — deterministic, no
     * API read required. The service selector targets a single pod via its
     * {@code kafka.node.id} label, ensuring clients reach the correct broker.
     */
    public void applyExternalServices(KafkaNodePool pool, String namespace, String clusterName,
            List<KafkaListenerSpec> externalListeners, int clusterIndex, KubernetesClient client) {
        String poolName = pool.getMetadata().getName();
        int replicas = pool.getSpec().getReplicas();

        for (int ordinal = 0; ordinal < replicas; ordinal++) {
            int nodeId = clusterIndex * KRaftConfigGenerator.BROKER_MULTIPLIER + ordinal;
            for (KafkaListenerSpec listener : externalListeners) {
                int nodePort = listener.getNodePortBase() + ordinal;
                String svcName = poolName + "-" + ordinal + "-" + listener.getName().toLowerCase() + "-ext";

                Service svc = new ServiceBuilder()
                        .withNewMetadata()
                            .withName(svcName)
                            .withNamespace(namespace)
                            .withLabels(Map.of(
                                KafkaPodSet.CLUSTER_LABEL,    clusterName,
                                KafkaPodSet.NODE_POOL_LABEL,  poolName,
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
                        .withNewSpec()
                            .withType("NodePort")
                            .withSelector(Map.of(
                                KafkaPodSet.NODE_ID_LABEL,   String.valueOf(nodeId),
                                KafkaPodSet.NODE_POOL_LABEL, poolName
                            ))
                            .addNewPort()
                                .withName(listener.getName().toLowerCase())
                                .withPort(listener.getPort())
                                .withTargetPort(new IntOrString(listener.getPort()))
                                .withNodePort(nodePort)
                            .endPort()
                        .endSpec()
                        .build();

                client.services().inNamespace(namespace).resource(svc).serverSideApply();
                LOG.debugf("Applied external service %s/%s (nodePort %d → node %d)",
                        namespace, svcName, nodePort, nodeId);
            }
        }
    }

    public void deleteExternalServices(KafkaNodePool pool, String namespace, KubernetesClient client) {
        String poolName = pool.getMetadata().getName();
        client.services().inNamespace(namespace)
              .withLabel(KafkaPodSet.NODE_POOL_LABEL, poolName)
              .withLabel(KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE)
              .list().getItems().stream()
              .filter(svc -> svc.getMetadata().getName().endsWith("-ext"))
              .forEach(svc -> client.services().inNamespace(namespace)
                                   .withName(svc.getMetadata().getName()).delete());
    }
}
