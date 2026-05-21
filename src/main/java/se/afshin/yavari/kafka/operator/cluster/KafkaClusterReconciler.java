package se.afshin.yavari.kafka.operator.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator;
import se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator;
import se.afshin.yavari.kafka.operator.upgrade.VersionUpgradeController;

import java.util.stream.Collectors;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ControllerConfiguration
@ApplicationScoped
public class KafkaClusterReconciler implements Reconciler<KafkaCluster>, Cleaner<KafkaCluster>,
        EventSourceInitializer<KafkaCluster> {

    private static final Logger LOG = Logger.getLogger(KafkaClusterReconciler.class);

    public static final String QUORUM_CONFIG_SUFFIX = "-quorum-config";

    @Inject
    KubernetesClient client;

    @Inject
    KRaftConfigGenerator kraftConfig;

    @Inject
    ClusterStatusAggregator statusAggregator;

    @Inject
    VersionUpgradeController versionUpgradeController;

    @Inject
    KafkaProxyOrchestrator proxyOrchestrator;

    @Inject
    ApicurioOrchestrator apicurioOrchestrator;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<KafkaCluster> context) {
        var podSetEventSource = new InformerEventSource<>(
            InformerConfiguration.from(KafkaPodSet.class, context)
                .withSecondaryToPrimaryMapper(podSet -> {
                    String ns = podSet.getMetadata().getNamespace();
                    String clusterName = podSet.getMetadata().getLabels()
                            .get(KafkaPodSet.CLUSTER_LABEL);
                    if (clusterName == null) return Set.of();
                    return Set.of(new ResourceID(clusterName, ns));
                })
                .build(),
            context);

        // Wake on changes to a referenced KafkaRbac (proxy reads rules from it).
        var rbacEventSource = new InformerEventSource<>(
            InformerConfiguration.from(KafkaRbac.class, context)
                .withSecondaryToPrimaryMapper(rbac -> {
                    String ns = rbac.getMetadata().getNamespace();
                    String rbacName = rbac.getMetadata().getName();
                    return context.getClient()
                            .resources(KafkaCluster.class).inNamespace(ns).list().getItems().stream()
                            .filter(c -> c.getSpec().getProxy() != null
                                    && rbacName.equals(c.getSpec().getProxy().getRbacRef()))
                            .map(c -> new ResourceID(c.getMetadata().getName(), ns))
                            .collect(Collectors.toSet());
                })
                .build(),
            context);

        // Wake on rotation of any Secret the proxy or apicurio mounts (cert-manager -> roll).
        var secretEventSource = new InformerEventSource<>(
            InformerConfiguration.from(Secret.class, context)
                .withSecondaryToPrimaryMapper(secret -> {
                    String ns = secret.getMetadata().getNamespace();
                    String secretName = secret.getMetadata().getName();
                    return context.getClient()
                            .resources(KafkaCluster.class).inNamespace(ns).list().getItems().stream()
                            .filter(c -> proxyReferencesSecret(c, secretName)
                                    || se.afshin.yavari.kafka.operator.apicurio.ApicurioOrchestrator
                                            .referencesSecret(c, secretName))
                            .map(c -> new ResourceID(c.getMetadata().getName(), ns))
                            .collect(Collectors.toSet());
                })
                .build(),
            context);

        return EventSourceInitializer.nameEventSources(podSetEventSource, rbacEventSource, secretEventSource);
    }

    static boolean proxyReferencesSecret(KafkaCluster c, String secretName) {
        if (c.getSpec().getProxy() == null) return false;
        KafkaProxyTlsConfig tls = c.getSpec().getProxy().getTls();
        String client = (tls != null && tls.getClientCertSecretRef() != null)
                ? tls.getClientCertSecretRef()
                : KafkaProxyOrchestrator.defaultClientCertSecret(KafkaProxyOrchestrator.PROXY_NAME);
        String server = (tls != null && tls.getServerCertSecretRef() != null)
                ? tls.getServerCertSecretRef()
                : KafkaProxyOrchestrator.defaultServerCertSecret(KafkaProxyOrchestrator.PROXY_NAME);
        return secretName.equals(client) || secretName.equals(server);
    }

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

        var allPods = client.pods().inNamespace(namespace)
                .withLabel(KafkaPodSet.CLUSTER_LABEL, name).list().getItems();
        versionUpgradeController.reconcile(cr, allPods, namespace, status);

        // Reconcile the Kroxylicious proxy sub-spec. The orchestrator does the same work the
        // old KafkaProxyReconciler did, but driven by cr.spec.proxy on the parent cluster.
        if (cr.getSpec().getProxy() != null) {
            status.setProxy(proxyOrchestrator.reconcile(cr, namespace, localClusterId));
        }
        // Reconcile the optional Apicurio sub-spec (Wave 4c).
        if (cr.getSpec().getApicurio() != null) {
            status.setApicurio(apicurioOrchestrator.reconcile(cr, namespace, localClusterId));
        }

        cr.setStatus(status);
        // Reschedule while any sub-status (proxy / apicurio) is still converging — the
        // sub-orchestrators set RECONCILING when they're waiting on dependent resources
        // (LB ingress, kafkasql journal topic, etc.) and need the cluster reconciler to
        // tick them again.
        boolean proxyConverging = status.getProxy() != null
                && status.getProxy().getPhase() == se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus.Phase.RECONCILING;
        boolean apicurioConverging = status.getApicurio() != null
                && status.getApicurio().getPhase() == se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus.Phase.RECONCILING;
        if (status.getPhase() != KafkaClusterStatus.Phase.READY
                || proxyConverging || apicurioConverging) {
            return UpdateControl.patchStatus(cr).rescheduleAfter(java.time.Duration.ofSeconds(15));
        }
        return UpdateControl.patchStatus(cr);
    }

    @Override
    public DeleteControl cleanup(KafkaCluster cr, Context<KafkaCluster> context) {
        String namespace = cr.getMetadata().getNamespace();
        String name = cr.getMetadata().getName();
        LOG.infof("KafkaCluster %s/%s deleted — ordered shutdown (brokers first, then controllers)", namespace, name);

        // Tear down proxy + apicurio resources up-front. Most are owner-ref'd to the cluster
        // and would cascade, but Deployment/ConfigMap explicit delete keeps order deterministic.
        if (cr.getSpec().getProxy() != null) {
            proxyOrchestrator.cleanup(cr, namespace);
        }
        if (cr.getSpec().getApicurio() != null) {
            apicurioOrchestrator.cleanup(cr, namespace);
        }

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
                    try { return Integer.parseInt(nodeIdStr) < KRaftConfigGenerator.CONTROLLER_BASE; }
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
