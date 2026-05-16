package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
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
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@ControllerConfiguration
@ApplicationScoped
public class KafkaClusterReconciler implements Reconciler<KafkaCluster> {

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

        try {
            kraftConfig.clusterIndex(cr.getSpec(), localClusterId);
        } catch (IllegalArgumentException e) {
            LOG.errorf("KAFKA_CLUSTER_ID '%s' not in spec.clusters — operator misconfigured", localClusterId);
            status.setPhase(KafkaClusterStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        if (cr.getSpec().getClusters().isEmpty()) {
            status.setPhase(KafkaClusterStatus.Phase.FAILED);
            status.setMessage("spec.clusters must not be empty");
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
