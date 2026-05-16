package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.cluster.ClusterStatusAggregator;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@ControllerConfiguration
@ApplicationScoped
public class KafkaClusterReconciler implements Reconciler<KafkaCluster>, Cleaner<KafkaCluster> {

    private static final Logger LOG = Logger.getLogger(KafkaClusterReconciler.class);

    static final String QUORUM_CONFIG_SUFFIX = "-quorum-config";

    @Inject
    KubernetesClient client;

    @Inject
    KRaftConfigGenerator kraftConfig;

    @Inject
    ClusterStatusAggregator statusAggregator;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public UpdateControl<KafkaCluster> reconcile(KafkaCluster cr, Context<KafkaCluster> context) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaCluster %s/%s (local cluster: %s)", namespace, name, localClusterId);

        KafkaClusterStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaClusterStatus();
        status.setLastReconcileTime(Instant.now().toString());
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        var validation = CrValidator.validateKafkaCluster(cr, localClusterId);
        if (!validation.valid()) {
            LOG.errorf("KafkaCluster %s/%s failed validation: %s", namespace, name, validation.message());
            status.setPhase(KafkaClusterStatus.Phase.FAILED);
            status.setMessage(validation.message());
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        String quorumVoters;
        try {
            quorumVoters = kraftConfig.buildQuorumVoters(cr.getSpec());
        } catch (IllegalArgumentException e) {
            status.setPhase(KafkaClusterStatus.Phase.FAILED);
            status.setMessage("Invalid spec: " + e.getMessage());
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        String clusterId = kraftConfig.clusterIdFrom(cr);
        applyQuorumConfigMap(cr, namespace, quorumVoters, clusterId);

        List<KafkaPodSet> podSets = client.resources(KafkaPodSet.class)
                .inNamespace(namespace)
                .withLabel(KafkaPodSet.CLUSTER_LABEL, name)
                .list()
                .getItems();

        statusAggregator.aggregate(status, podSets);

        cr.setStatus(status);
        if (status.getPhase() != KafkaClusterStatus.Phase.READY) {
            return UpdateControl.patchStatus(cr).rescheduleAfter(java.time.Duration.ofSeconds(15));
        }
        return UpdateControl.patchStatus(cr);
    }

    @Override
    public DeleteControl cleanup(KafkaCluster cr, Context<KafkaCluster> context) {
        String namespace = cr.getMetadata().getNamespace();
        String name = cr.getMetadata().getName();
        LOG.infof("KafkaCluster %s/%s deleted — ordered shutdown (brokers first, then controllers)", namespace, name);

        List<KafkaNodePool> pools = client.resources(KafkaNodePool.class)
                .inNamespace(namespace)
                .withLabel(KafkaNodePool.CLUSTER_LABEL, name)
                .list().getItems();

        // Delete broker pools not yet scheduled for deletion
        pools.stream()
             .filter(p -> p.getSpec().getRoles().contains(NodeRole.BROKER))
             .filter(p -> p.getMetadata().getDeletionTimestamp() == null)
             .forEach(p -> {
                 LOG.infof("Deleting broker pool %s", p.getMetadata().getName());
                 client.resources(KafkaNodePool.class).inNamespace(namespace)
                       .withName(p.getMetadata().getName()).delete();
             });

        // Wait until all broker pods are gone before touching controllers
        boolean brokerPodsExist = client.pods().inNamespace(namespace)
                .withLabel(KafkaPodSet.CLUSTER_LABEL, name)
                .list().getItems().stream()
                .anyMatch(p -> {
                    String nodeIdStr = p.getMetadata().getLabels()
                            .getOrDefault(KafkaPodSet.NODE_ID_LABEL, "-1");
                    try { return Integer.parseInt(nodeIdStr) < 1000; }
                    catch (NumberFormatException e) { return false; }
                });

        if (brokerPodsExist) {
            LOG.infof("KafkaCluster %s — broker pods still terminating, will retry", name);
            return DeleteControl.noFinalizerRemoval().rescheduleAfter(java.time.Duration.ofSeconds(15));
        }

        // Brokers gone — delete controller-only pools
        pools.stream()
             .filter(p -> !p.getSpec().getRoles().contains(NodeRole.BROKER))
             .filter(p -> p.getMetadata().getDeletionTimestamp() == null)
             .forEach(p -> {
                 LOG.infof("Deleting controller pool %s", p.getMetadata().getName());
                 client.resources(KafkaNodePool.class).inNamespace(namespace)
                       .withName(p.getMetadata().getName()).delete();
             });

        return DeleteControl.defaultDelete();
    }

    private void applyQuorumConfigMap(KafkaCluster cr, String namespace, String quorumVoters, String clusterId) {
        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(cr.getMetadata().getName() + QUORUM_CONFIG_SUFFIX)
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    cr.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(List.of(
                        new io.fabric8.kubernetes.api.model.OwnerReferenceBuilder()
                            .withApiVersion(cr.getApiVersion())
                            .withKind(cr.getKind())
                            .withName(cr.getMetadata().getName())
                            .withUid(cr.getMetadata().getUid())
                            .withController(true)
                            .withBlockOwnerDeletion(true)
                            .build()
                    ))
                .endMetadata()
                .addToData("controller.quorum.voters", quorumVoters)
                .addToData("cluster.id", clusterId)
                .build();

        client.configMaps().inNamespace(namespace).resource(cm).serverSideApply();
    }
}
