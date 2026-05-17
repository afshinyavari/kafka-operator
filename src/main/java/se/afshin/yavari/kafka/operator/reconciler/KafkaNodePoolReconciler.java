package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.nodepool.HeadlessServiceBuilder;
import se.afshin.yavari.kafka.operator.nodepool.PodTemplateFactory;
import se.afshin.yavari.kafka.operator.nodepool.PoolConfigMapBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ControllerConfiguration
@ApplicationScoped
public class KafkaNodePoolReconciler implements Reconciler<KafkaNodePool>, Cleaner<KafkaNodePool>,
        EventSourceInitializer<KafkaNodePool> {

    private static final Logger LOG = Logger.getLogger(KafkaNodePoolReconciler.class);

    @Inject KubernetesClient client;
    @Inject KRaftConfigGenerator kraftConfig;
    @Inject PoolConfigMapBuilder poolConfigMapBuilder;
    @Inject HeadlessServiceBuilder headlessServiceBuilder;
    @Inject PodTemplateFactory podTemplateFactory;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<KafkaNodePool> context) {
        var clusterEventSource = new InformerEventSource<>(
            InformerConfiguration.from(KafkaCluster.class, context)
                .withSecondaryToPrimaryMapper(cluster -> {
                    String ns = cluster.getMetadata().getNamespace();
                    String clusterName = cluster.getMetadata().getName();
                    return context.getClient()
                        .resources(KafkaNodePool.class)
                        .inNamespace(ns)
                        .withLabel(KafkaNodePool.CLUSTER_LABEL, clusterName)
                        .list().getItems().stream()
                        .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                        .collect(Collectors.toSet());
                })
                .build(),
            context);
        return EventSourceInitializer.nameEventSources(clusterEventSource);
    }

    @Override
    public UpdateControl<KafkaNodePool> reconcile(KafkaNodePool pool, Context<KafkaNodePool> context) {
        String poolName = pool.getMetadata().getName();
        String namespace = pool.getMetadata().getNamespace();
        String clusterName = pool.getMetadata().getLabels().get(KafkaNodePool.CLUSTER_LABEL);
        LOG.infof("Reconciling KafkaNodePool %s/%s (cluster: %s)", namespace, poolName, clusterName);

        KafkaNodePoolStatus status = pool.getStatus() != null ? pool.getStatus() : new KafkaNodePoolStatus();
        status.setPhase(KafkaNodePoolStatus.Phase.RECONCILING);
        status.setDesiredReplicas(pool.getSpec().getReplicas());

        var validation = CrValidator.validateKafkaNodePool(pool);
        if (!validation.valid()) {
            LOG.errorf("KafkaNodePool %s/%s failed validation: %s", namespace, poolName, validation.message());
            status.setPhase(KafkaNodePoolStatus.Phase.FAILED);
            status.setMessage(validation.message());
            pool.setStatus(status);
            return UpdateControl.patchStatus(pool);
        }

        KafkaCluster cluster = client.resources(KafkaCluster.class)
                .inNamespace(namespace).withName(clusterName).get();
        if (cluster == null) {
            status.setPhase(KafkaNodePoolStatus.Phase.FAILED);
            status.setMessage("Parent KafkaCluster '" + clusterName + "' not found");
            pool.setStatus(status);
            return UpdateControl.patchStatus(pool);
        }

        ConfigMap quorumCm = client.configMaps().inNamespace(namespace)
                .withName(clusterName + KafkaClusterReconciler.QUORUM_CONFIG_SUFFIX).get();
        if (quorumCm == null) {
            status.setPhase(KafkaNodePoolStatus.Phase.RECONCILING);
            status.setMessage("Quorum ConfigMap not ready yet — KafkaCluster may still be initialising");
            pool.setStatus(status);
            return UpdateControl.patchStatus(pool).rescheduleAfter(java.time.Duration.ofSeconds(10));
        }

        String quorumVoters = quorumCm.getData().get("controller.quorum.voters");
        String kafkaClusterId = quorumCm.getData().get("cluster.id");
        int clusterIndex = kraftConfig.clusterIndex(cluster.getSpec(), localClusterId);
        boolean isController = pool.getSpec().getRoles().contains(NodeRole.CONTROLLER);
        boolean isBroker = pool.getSpec().getRoles().contains(NodeRole.BROKER);
        String controllerAddr = cluster.getSpec().getClusters().get(clusterIndex).getControllerAdvertisedAddress();

        // Build and apply per-pool ConfigMap (server.properties + start script)
        ConfigMap poolCm = poolConfigMapBuilder.build(pool, cluster, namespace, clusterIndex, quorumVoters, controllerAddr);
        String configHash = Integer.toHexString(poolCm.getData().get("server.properties.template").hashCode());
        client.configMaps().inNamespace(namespace).resource(poolCm).serverSideApply();

        // Build and apply headless Service (+ optional ServiceExport for MCS)
        Service svc = headlessServiceBuilder.build(pool, namespace, clusterName, isController, isBroker);
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        headlessServiceBuilder.buildServiceExport(poolName + "-headless", namespace, pool)
                .ifPresent(export -> applyServiceExport(export, namespace, poolName));

        // Apply PodDisruptionBudget — maxUnavailable=1, no user config needed
        applyPdb(pool, namespace, clusterName);

        // Build desired pod list and apply KafkaPodSet
        List<PodEntry> desiredPods = podTemplateFactory.build(
                pool, cluster, namespace, clusterIndex, kafkaClusterId, configHash, isBroker, isController);
        applyPodSet(pool, namespace, clusterName, desiredPods);

        // Propagate KafkaPodSet readiness into pool status
        KafkaPodSet podSet = client.resources(KafkaPodSet.class)
                .inNamespace(namespace).withName(podSetName(pool)).get();
        if (podSet != null && podSet.getStatus() != null) {
            status.setReadyReplicas(podSet.getStatus().getReadyReplicas());
            if (podSet.getStatus().getReadyReplicas() >= pool.getSpec().getReplicas()) {
                status.setPhase(KafkaNodePoolStatus.Phase.READY);
            }
        }

        pool.setStatus(status);
        if (status.getPhase() != KafkaNodePoolStatus.Phase.READY) {
            return UpdateControl.patchStatus(pool).rescheduleAfter(java.time.Duration.ofSeconds(15));
        }
        return UpdateControl.patchStatus(pool);
    }

    @Override
    public DeleteControl cleanup(KafkaNodePool pool, Context<KafkaNodePool> context) {
        String namespace = pool.getMetadata().getNamespace();
        LOG.infof("KafkaNodePool %s deleted — KafkaPodSet will be garbage collected", pool.getMetadata().getName());
        client.configMaps().inNamespace(namespace).withName(pool.getMetadata().getName() + "-config").delete();
        client.services().inNamespace(namespace).withName(pool.getMetadata().getName() + "-headless").delete();
        client.policy().v1().podDisruptionBudget().inNamespace(namespace)
              .withName(pool.getMetadata().getName() + "-pdb").delete();
        if (mcsEnabled) {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                  .inNamespace(namespace)
                  .withName(pool.getMetadata().getName() + "-headless")
                  .delete();
        }
        return DeleteControl.defaultDelete();
    }

    private void applyPdb(KafkaNodePool pool, String namespace, String clusterName) {
        PodDisruptionBudget pdb = new PodDisruptionBudgetBuilder()
                .withNewMetadata()
                    .withName(pool.getMetadata().getName() + "-pdb")
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    clusterName,
                        KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(poolOwnerRef(pool))
                .endMetadata()
                .withNewSpec()
                    .withMaxUnavailable(new IntOrString(1))
                    .withNewSelector()
                        .withMatchLabels(Map.of(
                            KafkaPodSet.NODE_POOL_LABEL, pool.getMetadata().getName(),
                            KafkaPodSet.CLUSTER_LABEL,   clusterName
                        ))
                    .endSelector()
                .endSpec()
                .build();
        client.policy().v1().podDisruptionBudget().inNamespace(namespace).resource(pdb).serverSideApply();
    }

    private void applyServiceExport(GenericKubernetesResource export, String namespace, String poolName) {
        try {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                  .inNamespace(namespace).resource(export).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("ServiceExport CRD not available — MCS export skipped for %s: %s",
                    poolName + "-headless", e.getMessage());
        }
    }

    private void applyPodSet(KafkaNodePool pool, String namespace, String clusterName, List<PodEntry> desiredPods) {
        Map<String, String> selector = Map.of(
                KafkaPodSet.NODE_POOL_LABEL, pool.getMetadata().getName(),
                KafkaPodSet.CLUSTER_LABEL,   clusterName
        );

        KafkaPodSetSpec spec = new KafkaPodSetSpec();
        spec.setSelector(new LabelSelectorBuilder().withMatchLabels(selector).build());
        spec.setPods(desiredPods);

        KafkaPodSet podSet = new KafkaPodSet();
        podSet.setMetadata(new ObjectMetaBuilder()
                .withName(podSetName(pool))
                .withNamespace(namespace)
                .withLabels(Map.of(
                    KafkaPodSet.CLUSTER_LABEL,    clusterName,
                    KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                    KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                ))
                .withOwnerReferences(poolOwnerRef(pool))
                .build());
        podSet.setSpec(spec);

        client.resources(KafkaPodSet.class).inNamespace(namespace).resource(podSet).serverSideApply();
    }

    private String podSetName(KafkaNodePool pool) {
        return pool.getMetadata().getName() + "-podset";
    }

    private List<OwnerReference> poolOwnerRef(KafkaNodePool pool) {
        return List.of(new OwnerReferenceBuilder()
                .withApiVersion(pool.getApiVersion())
                .withKind(pool.getKind())
                .withName(pool.getMetadata().getName())
                .withUid(pool.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build());
    }
}
