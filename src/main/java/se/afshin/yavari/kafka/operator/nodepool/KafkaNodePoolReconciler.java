package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
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
import se.afshin.yavari.kafka.operator.cluster.CrValidator;
import se.afshin.yavari.kafka.operator.cluster.KafkaClusterReconciler;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.MetricsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetSpec;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    @Inject ExternalAccessServiceBuilder externalServiceBuilder;
    @Inject PodTemplateFactory podTemplateFactory;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject OptionalResourceApplier optionalApplier;

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

        // Re-reconcile all pools in the cluster when a proxy changes (mTLS affects all broker pools)
        var proxyEventSource = new InformerEventSource<>(
            InformerConfiguration.from(KafkaProxy.class, context)
                .withSecondaryToPrimaryMapper(proxy -> {
                    String ns = proxy.getMetadata().getNamespace();
                    return context.getClient()
                        .resources(KafkaNodePool.class)
                        .inNamespace(ns)
                        .withLabel(KafkaNodePool.CLUSTER_LABEL, proxy.getSpec().getClusterRef())
                        .list().getItems().stream()
                        .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                        .collect(Collectors.toSet());
                })
                .build(),
            context);

        // Wake the reconciler when a TLS Secret one of our pools mounts is rotated.
        // The configHash is recomputed each reconcile, so this is the trigger that turns
        // a cert-manager rotation into a rolling restart. We watch all Secrets and filter
        // by name in the mapper.
        var secretEventSource = new InformerEventSource<>(
            InformerConfiguration.from(Secret.class, context)
                .withSecondaryToPrimaryMapper(secret -> mapSecretToPools(secret, context.getClient()))
                .build(),
            context);

        return EventSourceInitializer.nameEventSources(clusterEventSource, proxyEventSource,
                secretEventSource);
    }

    /** For a changed Secret, find every NodePool in the same namespace that mounts it. */
    static Set<ResourceID> mapSecretToPools(Secret secret,
                                            io.fabric8.kubernetes.client.KubernetesClient client) {
        String ns = secret.getMetadata().getNamespace();
        String secretName = secret.getMetadata().getName();
        Set<ResourceID> matches = new HashSet<>();
        for (KafkaNodePool pool : client.resources(KafkaNodePool.class)
                .inNamespace(ns).list().getItems()) {
            String clusterName = pool.getMetadata().getLabels() == null
                    ? null
                    : pool.getMetadata().getLabels().get(KafkaNodePool.CLUSTER_LABEL);
            if (clusterName == null) continue;
            KafkaCluster cluster = client.resources(KafkaCluster.class)
                    .inNamespace(ns).withName(clusterName).get();
            if (cluster == null) continue;
            boolean isBroker = pool.getSpec().getRoles() != null
                    && pool.getSpec().getRoles().contains(NodeRole.BROKER);
            KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
            String brokerMtlsSecretName = null;
            if (isBroker && proxyMtls != null && proxyMtls.isEnabled()) {
                brokerMtlsSecretName = pool.getSpec().getBrokerCertSecretRef() != null
                        ? pool.getSpec().getBrokerCertSecretRef()
                        : pool.getMetadata().getName() + "-broker-tls";
            }
            if (mountedTlsSecretNames(pool, cluster, isBroker, brokerMtlsSecretName)
                    .contains(secretName)) {
                matches.add(new ResourceID(pool.getMetadata().getName(), ns));
            }
        }
        return matches;
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

        // Broker INTERNAL:SSL is driven by KafkaCluster.spec.proxyMtls — NOT by KafkaProxy presence —
        // so b/c clusters (which never see the cluster-a KafkaProxy CR) reconcile consistently.
        // The operator does NOT sign certs: the named secret must already exist (provisioned
        // externally by cert-manager in production, mcs-setup.sh in tests). Convention default
        // is "{poolName}-broker-tls"; override per pool via KafkaNodePoolSpec.brokerCertSecretRef.
        KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
        String proxyName = null;
        String brokerMtlsSecretName = null;
        if (proxyMtls != null && proxyMtls.isEnabled()) {
            // proxyName drives the authorizer + super.users block, which must apply to
            // controller-only pods as well (ACL writes in KRaft go through the controller).
            proxyName = proxyMtls.getProxyPrincipal();
            if (isBroker) {
                brokerMtlsSecretName = pool.getSpec().getBrokerCertSecretRef() != null
                        ? pool.getSpec().getBrokerCertSecretRef()
                        : poolName + "-broker-tls";
                if (client.secrets().inNamespace(namespace).withName(brokerMtlsSecretName).get() == null) {
                    String msg = "Broker mTLS secret '" + brokerMtlsSecretName + "' not found in namespace "
                            + namespace + " — waiting for cert-manager / mcs-setup to create it";
                    LOG.warnf(msg + " (pool %s)", poolName);
                    status.setPhase(KafkaNodePoolStatus.Phase.RECONCILING);
                    status.setMessage(msg);
                    pool.setStatus(status);
                    return UpdateControl.patchStatus(pool).rescheduleAfter(java.time.Duration.ofSeconds(10));
                }
            }
        }

        // Build and apply per-pool ConfigMap (server.properties + start script)
        ConfigMap poolCm = poolConfigMapBuilder.build(
                pool, cluster, namespace, clusterIndex, quorumVoters, controllerAddr, proxyName);
        // configHash drives the PodTemplate annotation and therefore any rolling restart.
        // Inputs: the rendered server.properties + start script, plus the resourceVersions of
        // every TLS Secret the pool's pods mount — so cert-manager rotations roll the pool.
        List<String> mountedSecrets = mountedTlsSecretNames(pool, cluster,
                isBroker, brokerMtlsSecretName);
        String secretRevisions = secretRevisionTracker.revisionsOf(mountedSecrets, namespace);
        String configHash = ConfigHasher.sha256(
                poolCm.getData().get("server.properties.template"),
                poolCm.getData().get("start.sh"),
                secretRevisions);
        client.configMaps().inNamespace(namespace).resource(poolCm).serverSideApply();

        // Build and apply headless Service (+ optional ServiceExport for MCS)
        Service svc = headlessServiceBuilder.build(pool, namespace, clusterName, isController, isBroker,
                cluster.getSpec().getListeners());
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        headlessServiceBuilder.buildServiceExport(poolName + "-headless", namespace, pool)
                .ifPresent(export -> serviceExportManager.apply(export, namespace, poolName + "-headless"));

        // Apply per-broker NodePort services for external listeners
        if (isBroker) {
            List<KafkaListenerSpec> externalListeners =
                    cluster.getSpec().getListeners().stream()
                        .filter(l -> l.getExternalAccess() != null)
                        .toList();
            if (!externalListeners.isEmpty()) {
                externalServiceBuilder.applyExternalServices(
                        pool, namespace, clusterName, externalListeners, clusterIndex, client);
            }
        }

        // Apply PodDisruptionBudget — maxUnavailable=1, no user config needed
        applyPdb(pool, namespace, clusterName);

        // Apply metrics Service + ServiceMonitor when metricsConfig is enabled
        MetricsConfig metrics = cluster.getSpec().getMetricsConfig();
        if (metrics != null && metrics.getConfigMapRef() != null) {
            applyMetricsService(pool, namespace, clusterName);
            applyServiceMonitor(pool, namespace, clusterName);
        }

        // Build desired pod list and apply KafkaPodSet
        List<PodEntry> desiredPods = podTemplateFactory.build(
                pool, cluster, namespace, clusterIndex, kafkaClusterId, configHash,
                isBroker, isController, brokerMtlsSecretName);
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
        externalServiceBuilder.deleteExternalServices(pool, namespace, client);
        client.policy().v1().podDisruptionBudget().inNamespace(namespace)
              .withName(pool.getMetadata().getName() + "-pdb").delete();
        client.services().inNamespace(namespace)
              .withName(pool.getMetadata().getName() + "-metrics").delete();
        optionalApplier.deleteServiceMonitor(pool.getMetadata().getName() + "-metrics", namespace);
        if (mcsEnabled) {
            serviceExportManager.delete(pool.getMetadata().getName() + "-headless", namespace);
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

    private void applyMetricsService(KafkaNodePool pool, String namespace, String clusterName) {
        Service svc = new ServiceBuilder()
                .withNewMetadata()
                    .withName(pool.getMetadata().getName() + "-metrics")
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    clusterName,
                        KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(poolOwnerRef(pool))
                .endMetadata()
                .withNewSpec()
                    .withSelector(Map.of(
                        KafkaPodSet.NODE_POOL_LABEL, pool.getMetadata().getName(),
                        KafkaPodSet.CLUSTER_LABEL,   clusterName))
                    .addNewPort()
                        .withName("jmx")
                        .withPort(9101)
                        .withTargetPort(new IntOrString(9101))
                    .endPort()
                .endSpec()
                .build();
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
    }

    private void applyServiceMonitor(KafkaNodePool pool, String namespace, String clusterName) {
        Map<String, Object> spec = Map.of(
            "selector", Map.of("matchLabels", Map.of(
                KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                KafkaPodSet.CLUSTER_LABEL,    clusterName,
                KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE)),
            "endpoints", List.of(Map.of("port", "jmx", "interval", "30s")));
        GenericKubernetesResource sm = new GenericKubernetesResourceBuilder()
                .withApiVersion("monitoring.coreos.com/v1")
                .withKind("ServiceMonitor")
                .withNewMetadata()
                    .withName(pool.getMetadata().getName() + "-metrics")
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    clusterName,
                        KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(poolOwnerRef(pool))
                .endMetadata()
                .addToAdditionalProperties("spec", spec)
                .build();
        optionalApplier.applyServiceMonitor(sm, namespace);
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

    /** TLS Secret names this pool's pods mount — fed into configHash so cert-manager rotations
     *  trigger a rolling restart. Mirrors PodTemplateFactory's volume-build logic:
     *  the shared broker-mTLS secret (when proxyMtls is on for a broker pool), and the
     *  per-pod {@code {podName}-tls} secrets (when controllerTls is set or any listener has TLS). */
    static List<String> mountedTlsSecretNames(KafkaNodePool pool, KafkaCluster cluster,
                                              boolean isBroker, String brokerMtlsSecretName) {
        List<String> names = new ArrayList<>();
        if (brokerMtlsSecretName != null) {
            names.add(brokerMtlsSecretName);
        }
        List<KafkaListenerSpec> listeners = cluster.getSpec().getListeners();
        boolean hasTlsListener = listeners != null
                && listeners.stream().anyMatch(l -> l.getTls() != null);
        boolean needsTls = cluster.getSpec().getControllerTls() != null
                || (isBroker && hasTlsListener);
        if (needsTls) {
            String poolName = pool.getMetadata().getName();
            for (int i = 0; i < pool.getSpec().getReplicas(); i++) {
                names.add(poolName + "-" + i + "-tls");
            }
        }
        return names;
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
