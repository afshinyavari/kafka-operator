package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;
import se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The Kroxylicious proxy reconcile loop, extracted from the deleted KafkaProxyReconciler.
 * Called by {@link se.afshin.yavari.kafka.operator.cluster.KafkaClusterReconciler} now that
 * the proxy is a sub-spec of KafkaCluster (Wave 4b of the audit).
 *
 * <p>To avoid touching every builder that took {@code KafkaProxy} as a parameter, this class
 * synthesises a {@code KafkaProxy} instance from the parent cluster's {@code spec.proxy}
 * field. The synthetic instance is never written to the API server.
 *
 * <p>Derived from {@code KafkaCluster.spec} (no longer carried on the user-facing sub-spec):
 * <ul>
 *   <li>{@code mcsEnabled} — true when {@code clusters.size() > 1}.
 *   <li>{@code targetClusters} — every {@code spec.clusters[].id}.
 *   <li>{@code clusterRef} — the parent CR name.
 *   <li>{@code poolRef} — the cluster's broker KafkaNodePool (looked up by label).
 *   <li>{@code brokerNodeIdRanges} — built from broker pool replicas + the operator's
 *       deterministic {@code clusterIndex * 1000 + i} node-id scheme.
 * </ul>
 */
@ApplicationScoped
public class KafkaProxyOrchestrator {

    /** Fixed name for the proxy's Deployment / Service / ConfigMap regardless of the parent
     *  KafkaCluster's name. Matches the legacy KafkaProxy CR name that the kind test scripts
     *  hardcode in ~12 places. Multi-KafkaCluster-per-namespace would need a per-cluster
     *  suffix here — follow-up. */
    public static final String PROXY_NAME = "kafka-proxy";

    private static final Logger LOG = Logger.getLogger(KafkaProxyOrchestrator.class);

    @Inject KubernetesClient client;
    @Inject KroxyliciousConfigBuilder configBuilder;
    @Inject ProxyDeploymentBuilder deploymentBuilder;
    @Inject ProxyServiceBuilder serviceBuilder;
    @Inject ExternalAccessResolver externalAccessResolver;
    @Inject TLSRouteBuilder tlsRouteBuilder;
    @Inject IngressBuilder ingressBuilder;
    @Inject CrossClusterRollCoordinator rollCoordinator;
    @Inject ProxyRollTracker rollTracker;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject OptionalResourceApplier optionalApplier;

    /** Reconciles the proxy described by {@code cr.spec.proxy}. Returns the new sub-status. */
    public KafkaProxyStatus reconcile(KafkaCluster cr, String namespace, String localClusterId) {
        String name = PROXY_NAME;
        String clusterName = cr.getMetadata().getName();
        KafkaClusterProxySpec proxySpec = cr.getSpec().getProxy();
        LOG.infof("Reconciling proxy for KafkaCluster %s/%s", namespace, clusterName);

        KafkaProxyStatus status = cr.getStatus() != null && cr.getStatus().getProxy() != null
                ? cr.getStatus().getProxy() : new KafkaProxyStatus();
        status.setPhase(KafkaProxyStatus.Phase.RECONCILING);

        // proxyMtls is mandatory for the merged proxy (Wave 4a already removed the toggle).
        KafkaProxyMtlsConfig proxyMtls = cr.getSpec().getProxyMtls();
        if (proxyMtls == null) {
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage("spec.proxyMtls is required when spec.proxy is set");
            return status;
        }

        // MCS derived from cluster topology.
        boolean mcsEnabled = cr.getSpec().getClusters() != null
                && cr.getSpec().getClusters().size() > 1;
        List<String> targetClusters = cr.getSpec().getClusters() == null ? List.of()
                : cr.getSpec().getClusters().stream()
                    .map(ClusterEntry::getId).collect(Collectors.toList());

        if (mcsEnabled && !targetClusters.contains(localClusterId)) {
            LOG.infof("KafkaCluster %s/%s: local cluster '%s' not in spec.clusters %s — skipping proxy",
                    namespace, name, localClusterId, targetClusters);
            status.setPhase(KafkaProxyStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + localClusterId + "' is not in spec.clusters");
            status.setReadyReplicas(0);
            return status;
        }

        // Resolve broker count + node id base + broker pool name.
        BrokerInfo brokers;
        try {
            brokers = resolveBrokers(cr, namespace);
        } catch (IllegalStateException e) {
            status.setMessage(e.getMessage());
            return status;
        }

        // Resolve proxy client + server cert secret names (spec overrides, else defaults).
        KafkaProxyTlsConfig tls = proxySpec.getTls();
        String clientCertSecret = (tls != null && tls.getClientCertSecretRef() != null)
                ? tls.getClientCertSecretRef() : name + "-client-tls";
        String serverCertSecret = (tls != null && tls.getServerCertSecretRef() != null)
                ? tls.getServerCertSecretRef() : name + "-server-tls";
        for (String s : new String[] { clientCertSecret, serverCertSecret }) {
            if (client.secrets().inNamespace(namespace).withName(s).get() == null) {
                String msg = "Proxy TLS secret '" + s + "' not found in namespace " + namespace
                        + " — waiting for cert-manager / mcs-setup to create it";
                LOG.warnf(msg);
                status.setMessage(msg);
                return status;
            }
        }

        // Synthesise a KafkaProxy for the existing builders.
        KafkaProxy syn = synthetic(cr, name, namespace, brokers, mcsEnabled, targetClusters);

        // Resolve external access (LB ingress / Gateway / Ingress hostname).
        ExternalAccessResolution external;
        try {
            external = externalAccessResolver.resolve(syn, localClusterId, namespace, client);
        } catch (IllegalStateException e) {
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
            return status;
        }

        try {
            // Generate and apply config ConfigMap.
            String configYaml = configBuilder.build(syn, brokers.count, brokers.nodeIdBase,
                    namespace, mcsEnabled, external);
            String secretRevisions = secretRevisionTracker.revisionsOf(
                    List.of(clientCertSecret, serverCertSecret), namespace);
            String configHash = ConfigHasher.sha256(configYaml, secretRevisions);
            ConfigMap configMap = new ConfigMapBuilder()
                    .withNewMetadata()
                        .withName(name + "-config")
                        .withNamespace(namespace)
                        .withOwnerReferences(clusterOwnerRef(cr))
                    .endMetadata()
                    .withData(Map.of("config.yaml", configYaml))
                    .build();
            client.configMaps().inNamespace(namespace).resource(configMap).createOrReplace();

            // Cross-cluster roll gate.
            Deployment existing = client.apps().deployments()
                    .inNamespace(namespace).withName(name).get();
            if (existing != null
                    && cr.getSpec().getClusterRollOrder() != null
                    && !cr.getSpec().getClusterRollOrder().isEmpty()) {
                boolean rollIsRequired = rollWillHappen(existing, proxySpec.getImage(), configHash);
                if (rollIsRequired && !rollCoordinator.isMyTurnToRoll(cr.getSpec(), localClusterId)) {
                    LOG.infof("Proxy for %s/%s: waiting for preceding cluster per clusterRollOrder %s",
                            namespace, name, cr.getSpec().getClusterRollOrder());
                    status.setMessage("Waiting for preceding cluster per spec.clusterRollOrder "
                            + "before rolling proxy (image or config change)");
                    return status;
                }
            }
            if (rollWillHappen(existing, proxySpec.getImage(), configHash)) {
                rollTracker.markRolling(namespace, name);
            }

            // Apply Deployment + Service.
            Deployment deployment = deploymentBuilder.build(syn, namespace, configHash);
            attachOwnerRef(deployment.getMetadata(), clusterOwnerRef(cr));
            client.apps().deployments().inNamespace(namespace).resource(deployment).serverSideApply();

            Service service = serviceBuilder.build(syn, brokers.count, namespace, external);
            attachOwnerRef(service.getMetadata(), clusterOwnerRef(cr));
            client.services().inNamespace(namespace).resource(service).serverSideApply();

            if (mcsEnabled) {
                serviceExportManager.apply(name, namespace);
            }

            if (external.type() == ExternalAccessType.GATEWAY) {
                GenericKubernetesResource route = tlsRouteBuilder.build(
                        syn, brokers.count, namespace, external);
                optionalApplier.applyTlsRoute(route, namespace);
            }
            if (external.type() == ExternalAccessType.INGRESS) {
                var ingress = ingressBuilder.build(syn, brokers.count, namespace, external);
                optionalApplier.applyIngress(ingress, namespace);
            }

            if (external.isPending()) {
                LOG.infof("Proxy for %s/%s: LoadBalancer ingress not yet assigned — rescheduling",
                        namespace, name);
                status.setMessage("Waiting for LoadBalancer ingress address...");
                return status;
            }

            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= proxySpec.getReplicas()) {
                status.setPhase(KafkaProxyStatus.Phase.READY);
                status.setMessage(null);
                rollTracker.markComplete(namespace, name);
            } else {
                status.setMessage("Waiting for proxy pods: " + ready + "/" + proxySpec.getReplicas());
            }
        } catch (Exception e) {
            LOG.errorf("Proxy for %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }
        return status;
    }

    /** Cleanup-side delete of the proxy resources owned by this cluster. Called from
     *  KafkaClusterReconciler.cleanup() — owner refs handle most of it, but Deployment +
     *  ConfigMap may need explicit deletion before the cluster CR is finalized. */
    public void cleanup(KafkaCluster cr, String namespace) {
        String name = PROXY_NAME;
        client.configMaps().inNamespace(namespace).withName(name + "-config").delete();
        client.apps().deployments().inNamespace(namespace).withName(name).delete();
        client.services().inNamespace(namespace).withName(name).delete();
        boolean mcsEnabled = cr.getSpec().getClusters() != null
                && cr.getSpec().getClusters().size() > 1;
        if (mcsEnabled) {
            serviceExportManager.delete(name, namespace);
        }
        optionalApplier.deleteTlsRoute(name, namespace);
        optionalApplier.deleteIngress(name, namespace);
    }

    /** True if any KafkaProxy in the namespace references the given Secret name. With the
     *  merger this is now used by KafkaClusterReconciler's Secret informer. */
    public static String defaultClientCertSecret(String clusterName) {
        return clusterName + "-client-tls";
    }
    public static String defaultServerCertSecret(String clusterName) {
        return clusterName + "-server-tls";
    }

    private record BrokerInfo(int count, int nodeIdBase, String poolName, List<BrokerNodeIdRange> ranges) {}

    private BrokerInfo resolveBrokers(KafkaCluster cr, String namespace) {
        String clusterName = cr.getMetadata().getName();
        List<KafkaNodePool> brokerPools = client.resources(KafkaNodePool.class)
                .inNamespace(namespace)
                .withLabel(KafkaNodePool.CLUSTER_LABEL, clusterName)
                .list().getItems().stream()
                .filter(p -> p.getSpec().getRoles() != null
                        && p.getSpec().getRoles().contains(NodeRole.BROKER))
                .toList();
        if (brokerPools.isEmpty()) {
            throw new IllegalStateException("No broker KafkaNodePool found for cluster '"
                    + clusterName + "'");
        }
        // Local broker count = local pool's replicas. For MCS the proxy needs broker ranges
        // across all clusters; the operator's deterministic scheme is
        //   broker node id = clusterIndex * 1000 + poolLocalIndex
        // so for N replicas per cluster and K clusters, the ranges are
        //   [0..N-1], [1000..1000+N-1], ..., [(K-1)*1000..(K-1)*1000+N-1].
        // (We assume uniform broker count per cluster, which is the typical MCS topology.)
        int localBrokerCount = brokerPools.get(0).getSpec().getReplicas();
        String poolName = brokerPools.get(0).getMetadata().getName();
        boolean mcsEnabled = cr.getSpec().getClusters() != null
                && cr.getSpec().getClusters().size() > 1;
        int clusterCount = mcsEnabled ? cr.getSpec().getClusters().size() : 1;
        List<BrokerNodeIdRange> ranges = new ArrayList<>(clusterCount);
        for (int i = 0; i < clusterCount; i++) {
            BrokerNodeIdRange r = new BrokerNodeIdRange();
            // Kroxylicious's NamedRange constructor requires non-null name. Use the cluster
            // id (lowercased) so the names match `brokers-{a,b,c}` style.
            String clusterId = mcsEnabled
                    ? cr.getSpec().getClusters().get(i).getId().toLowerCase()
                    : "local";
            r.setName("brokers-" + clusterId);
            r.setStart(i * 1000);
            r.setEnd(i * 1000 + localBrokerCount - 1);
            ranges.add(r);
        }
        return new BrokerInfo(
                localBrokerCount * clusterCount,        // total broker count across all clusters
                0,                                       // lowest broker id is always 0
                poolName,
                ranges);
    }

    private KafkaProxy synthetic(KafkaCluster cr, String name, String namespace,
                                  BrokerInfo brokers, boolean mcsEnabled, List<String> targets) {
        KafkaClusterProxySpec p = cr.getSpec().getProxy();
        KafkaProxy syn = new KafkaProxy();
        syn.setMetadata(new ObjectMetaBuilder()
                .withName(name).withNamespace(namespace)
                .withOwnerReferences(clusterOwnerRef(cr))
                .build());
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setClusterRef(name);
        spec.setPoolRef(brokers.poolName);
        spec.setReplicas(p.getReplicas());
        spec.setImage(p.getImage());
        spec.setClientPort(p.getClientPort());
        spec.setRbacRef(p.getRbacRef());
        spec.setApicurioRef(p.getApicurioRef());
        spec.setTls(p.getTls());
        spec.setOidc(p.getOidc());
        spec.setFilters(p.getFilters());
        spec.setCustomFilters(p.getCustomFilters());
        spec.setExternalAccess(p.getExternalAccess());
        spec.setBrokerNodeIdRanges(brokers.ranges);
        spec.setTargetClusters(targets);
        if (mcsEnabled) {
            McsConfig mcs = new McsConfig();
            mcs.setEnabled(true);
            spec.setMcs(mcs);
        }
        syn.setSpec(spec);
        return syn;
    }

    private List<OwnerReference> clusterOwnerRef(KafkaCluster cr) {
        return List.of(new OwnerReferenceBuilder()
                .withApiVersion(cr.getApiVersion())
                .withKind(cr.getKind())
                .withName(cr.getMetadata().getName())
                .withUid(cr.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build());
    }

    private void attachOwnerRef(ObjectMeta meta, List<OwnerReference> owner) {
        if (meta == null) return;
        if (meta.getOwnerReferences() == null || meta.getOwnerReferences().isEmpty()) {
            meta.setOwnerReferences(owner);
        }
    }

    private static boolean rollWillHappen(Deployment existing, String desiredImage, String desiredHash) {
        if (existing == null) return true;
        if (existing.getSpec() == null || existing.getSpec().getTemplate() == null) return true;
        var podSpec = existing.getSpec().getTemplate().getSpec();
        String existingImage = (podSpec != null
                && podSpec.getContainers() != null
                && !podSpec.getContainers().isEmpty())
            ? podSpec.getContainers().get(0).getImage()
            : null;
        if (!desiredImage.equals(existingImage)) return true;
        var meta = existing.getSpec().getTemplate().getMetadata();
        String existingHash = (meta != null && meta.getAnnotations() != null)
            ? meta.getAnnotations().getOrDefault(ProxyDeploymentBuilder.CONFIG_HASH_ANNOTATION, "")
            : "";
        return !desiredHash.equals(existingHash);
    }

    private int readyReplicas(String deploymentName, String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace)
                .withName(deploymentName).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }
}
