package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistrySpec;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterApicurioSpec;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressBuilder;
import se.afshin.yavari.kafka.operator.externalaccess.HttpRouteBuilder;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.OwnerReferences;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolution;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver;
import se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator;
import se.afshin.yavari.kafka.operator.rolling.RollTracker;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Apicurio Registry reconcile loop, extracted from the deleted ApicurioRegistryReconciler.
 * Called by {@link se.afshin.yavari.kafka.operator.cluster.KafkaClusterReconciler} when
 * {@code spec.apicurio != null} (Wave 4c of the audit).
 *
 * <p>Same synthetic-CR strategy as
 * {@link se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator}: the orchestrator
 * builds an {@link ApicurioRegistry} from the parent cluster's sub-spec and hands it to the
 * existing builders (ApicurioDeploymentBuilder, ApicurioProxyContainerBuilder,
 * ApicurioProxyServiceBuilder, ApicurioKafkasqlSupport, HttpIngressBuilder, HttpRouteBuilder)
 * so the builder code didn't have to change shape.
 *
 * <p>Derived from {@code KafkaCluster.spec}:
 * <ul>
 *   <li>{@code mcsEnabled} — true when {@code clusters.size() > 1}.
 *   <li>{@code targetClusters} — every {@code spec.clusters[].id}.
 *   <li>{@code storage.clusterRef} — the parent CR name.
 *   <li>{@code exportService} — always-on under MCS.
 * </ul>
 */
@ApplicationScoped
public class ApicurioOrchestrator {

    /** Fixed name for the Apicurio resources regardless of parent KafkaCluster name. Matches
     *  the legacy ApicurioRegistry CR name and the ~3 e2e scripts that key on it. */
    public static final String APICURIO_NAME = "apicurio";

    private static final Logger LOG = Logger.getLogger(ApicurioOrchestrator.class);

    @Inject KubernetesClient client;
    @Inject ApicurioDeploymentBuilder deploymentBuilder;
    @Inject ApicurioProxyContainerBuilder proxyContainerBuilder;
    @Inject se.afshin.yavari.kafka.operator.audit.AuditOrchestrator auditOrchestrator;
    @Inject ApicurioProxyServiceBuilder proxyServiceBuilder;
    @Inject ApicurioKafkasqlSupport kafkasqlSupport;
    @Inject ExternalAccessResolver externalAccessResolver;
    @Inject HttpIngressBuilder httpIngressBuilder;
    @Inject HttpRouteBuilder httpRouteBuilder;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject OptionalResourceApplier optionalApplier;
    @Inject se.afshin.yavari.kafka.operator.infra.MetricsResources metricsResources;
    @Inject se.afshin.yavari.kafka.operator.infra.PdbBuilder pdbBuilder;
    @Inject CrossClusterRollCoordinator rollCoordinator;
    @Inject RollTracker rollTracker;

    /** Reconciles the registry described by {@code cr.spec.apicurio}. Returns the new sub-status. */
    public ApicurioRegistryStatus reconcile(KafkaCluster cr, String namespace, String localClusterId) {
        String clusterName = cr.getMetadata().getName();
        KafkaClusterApicurioSpec spec = cr.getSpec().getApicurio();
        LOG.infof("Reconciling Apicurio for KafkaCluster %s/%s", namespace, clusterName);

        ApicurioRegistryStatus status = cr.getStatus() != null && cr.getStatus().getApicurio() != null
                ? cr.getStatus().getApicurio() : new ApicurioRegistryStatus();
        status.setPhase(ApicurioRegistryStatus.Phase.RECONCILING);

        boolean mcsEnabled = cr.getSpec().getClusters() != null
                && cr.getSpec().getClusters().size() > 1;
        List<String> targetClusters = cr.getSpec().getClusters() == null ? List.of()
                : cr.getSpec().getClusters().stream()
                    .map(ClusterEntry::getId).collect(Collectors.toList());

        if (mcsEnabled && !targetClusters.contains(localClusterId)) {
            LOG.infof("KafkaCluster %s/%s: local cluster '%s' not in spec.clusters %s — skipping Apicurio",
                    namespace, clusterName, localClusterId, targetClusters);
            status.setPhase(ApicurioRegistryStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + localClusterId + "' is not in spec.clusters");
            return status;
        }

        String rbacRef = spec.getRbacRef();
        boolean proxyEnabled = rbacRef != null && spec.getRbacProxyImage() != null;
        HttpExternalAccessConfig ea = spec.getExternalAccess();

        if (ea != null && ea.getType() != null && !proxyEnabled) {
            status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
            status.setMessage("externalAccess requires spec.apicurio.rbacRef + rbacProxyImage — "
                    + "only the rbac-proxy is safe to expose externally");
            return status;
        }
        if (rbacRef != null) {
            ConfigMap policyMap = client.configMaps().inNamespace(namespace)
                    .withName(rbacRef + "-apicurio-policy").get();
            if (policyMap == null) {
                status.setMessage("Waiting for KafkaRbac '" + rbacRef + "' to be reconciled");
                return status;
            }
        }

        ApicurioRegistry syn = synthetic(cr, namespace, mcsEnabled, targetClusters);

        try {
            ApicurioDeploymentBuilder.KafkasqlConfig kafkasqlConfig = null;
            if (spec.getStorage() != null && "kafkasql".equals(spec.getStorage().getType())) {
                ApicurioKafkasqlSupport.Result result = kafkasqlSupport.prepare(syn);
                if (result instanceof ApicurioKafkasqlSupport.Result.Failed f) {
                    status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
                    status.setMessage(f.message());
                    return status;
                }
                if (result instanceof ApicurioKafkasqlSupport.Result.Pending p) {
                    status.setMessage(p.reason());
                    return status;
                }
                kafkasqlConfig = ((ApicurioKafkasqlSupport.Result.Ready) result).config();
            }

            Container proxyContainer = null;
            Volume policyVolume = null;
            if (proxyEnabled) {
                proxyContainer = proxyContainerBuilder.build(syn);
                policyVolume = proxyContainerBuilder.policyVolume(rbacRef);
                // Inject audit env + tls mount on the rbac-proxy container when the Kafka-topic
                // sink is enabled (no-op otherwise).
                auditOrchestrator.injectIntoContainer(proxyContainer, cr, namespace);
            }

            String secretRevisions = "";
            if (kafkasqlConfig != null && kafkasqlConfig.tlsSecretRef() != null) {
                secretRevisions = secretRevisionTracker.revisionsOf(
                        List.of(kafkasqlConfig.tlsSecretRef()), namespace);
            }
            String configHash = ConfigHasher.sha256(secretRevisions);
            Deployment dep = deploymentBuilder.build(syn, namespace, proxyContainer,
                    policyVolume, kafkasqlConfig, configHash);
            attachOwnerRef(dep, cr);

            // Cross-cluster roll gate (same shape as KafkaProxyOrchestrator). A synchronized
            // kafkasql cert rotation across all 3 MCS clusters would otherwise roll every
            // Apicurio replica at once — clusterRollOrder serialises them.
            String depName = APICURIO_NAME + "-registry";
            Deployment existing = client.apps().deployments()
                    .inNamespace(namespace).withName(depName).get();
            if (existing != null
                    && cr.getSpec().getClusterRollOrder() != null
                    && !cr.getSpec().getClusterRollOrder().isEmpty()) {
                boolean rollIsRequired = rollWillHappen(existing, spec.getImage(), configHash);
                if (rollIsRequired && !rollCoordinator.isMyTurnToRoll(cr.getSpec(), localClusterId)) {
                    LOG.infof("Apicurio for %s/%s: waiting for preceding cluster per clusterRollOrder %s",
                            namespace, clusterName, cr.getSpec().getClusterRollOrder());
                    status.setMessage("Waiting for preceding cluster per spec.clusterRollOrder "
                            + "before rolling Apicurio (image or config change)");
                    return status;
                }
            }
            if (rollWillHappen(existing, spec.getImage(), configHash)) {
                rollTracker.markRolling("apicurio", namespace, depName);
            }

            // Add the audit-tls Volume to the pod template when the Kafka sink is enabled.
            // The container-side mount was already added above; this is the matching pod volume.
            auditOrchestrator.injectIntoDeployment(dep, cr, namespace);
            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            String proxySvcName = APICURIO_NAME + "-rbac-proxy";
            if (proxyEnabled) {
                Service proxySvc = proxyServiceBuilder.build(syn, namespace);
                attachOwnerRefSvc(proxySvc, cr);
                client.services().inNamespace(namespace).resource(proxySvc).serverSideApply();
            }

            // Always-export under MCS (the synthetic CR has exportService=true). Single-cluster
            // mode skips the export.
            boolean wantExport = proxyEnabled && mcsEnabled;
            if (wantExport) {
                serviceExportManager.apply(proxySvcName, namespace);
            }

            ExternalAccessResolution external = ExternalAccessResolution.internal();
            if (proxyEnabled && ea != null && ea.getType() != null) {
                try {
                    external = externalAccessResolver.resolve(ea, proxySvcName, localClusterId,
                            namespace, client);
                } catch (IllegalStateException e) {
                    status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
                    status.setMessage(e.getMessage());
                    return status;
                }
                applyOrDeleteIngress(syn, ea, external);
                applyOrDeleteHttpRoute(syn, ea, external);
                if (external.advertisedHost() != null) {
                    String scheme = isTls(ea) ? "https" : "http";
                    int port = scheme.equals("https") ? 443 : ApicurioProxyContainerBuilder.PROXY_PORT;
                    String hostPort = (ea.getType() == ExternalAccessType.LOADBALANCER)
                            ? external.advertisedHost() + ":" + ApicurioProxyContainerBuilder.PROXY_PORT
                            : external.advertisedHost() + (port == 80 || port == 443 ? "" : ":" + port);
                    status.setExternalUrl(scheme + "://" + hostPort);
                }
            } else {
                optionalApplier.deleteIngress(proxySvcName, namespace);
                optionalApplier.deleteHttpRoute(proxySvcName, namespace);
                status.setExternalUrl(null);
            }

            if (proxyEnabled) {
                String proxyUrl = "http://" + proxySvcName + "." + namespace
                        + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
                status.setProxyUrl(proxyUrl);
            }

            boolean rescheduleForLb = external.isPending();
            if (rescheduleForLb) {
                status.setMessage("Registry ready; waiting for LoadBalancer ingress address");
            }

            applyOrDeleteMetrics(cr, namespace);

            pdbBuilder.apply(APICURIO_NAME + "-registry", namespace,
                    ApicurioDeploymentBuilder.labels(APICURIO_NAME),
                    ApicurioDeploymentBuilder.labels(APICURIO_NAME),
                    spec.getReplicas(), cr);

            int ready = readyReplicas(APICURIO_NAME + "-registry", namespace);
            if (ready >= spec.getReplicas()) {
                status.setPhase(ApicurioRegistryStatus.Phase.READY);
                if (!rescheduleForLb) status.setMessage(null);
                rollTracker.markComplete("apicurio", namespace, APICURIO_NAME + "-registry");
            } else {
                status.setMessage("Waiting for registry pods: " + ready + "/" + spec.getReplicas());
            }
        } catch (Exception e) {
            LOG.errorf("Apicurio for %s/%s failed: %s", namespace, clusterName, e.getMessage());
            status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }
        return status;
    }

    public void cleanup(KafkaCluster cr, String namespace) {
        String name = APICURIO_NAME;
        String proxySvcName = name + "-rbac-proxy";
        kafkasqlSupport.cleanup(synthetic(cr, namespace,
                cr.getSpec().getClusters() != null && cr.getSpec().getClusters().size() > 1,
                cr.getSpec().getClusters() == null ? List.of()
                        : cr.getSpec().getClusters().stream()
                            .map(ClusterEntry::getId).collect(Collectors.toList())));
        client.apps().deployments().inNamespace(namespace).withName(name + "-registry").delete();
        client.services().inNamespace(namespace).withName(proxySvcName).delete();
        optionalApplier.deleteIngress(proxySvcName, namespace);
        optionalApplier.deleteHttpRoute(proxySvcName, namespace);
        client.services().inNamespace(namespace)
              .withName(name + se.afshin.yavari.kafka.operator.infra.MetricsResources.METRICS_SUFFIX)
              .delete();
        optionalApplier.deleteServiceMonitor(
                name + se.afshin.yavari.kafka.operator.infra.MetricsResources.METRICS_SUFFIX, namespace);
        pdbBuilder.delete(name + "-registry", namespace);
        boolean mcsEnabled = cr.getSpec().getClusters() != null
                && cr.getSpec().getClusters().size() > 1;
        if (mcsEnabled) {
            serviceExportManager.delete(proxySvcName, namespace);
        }
    }

    /** True if the cluster's apicurio storage points at the given Secret name. Used by the
     *  KafkaClusterReconciler's Secret informer mapper so a kafkasql cert rotation wakes the
     *  reconciler. */
    public static boolean referencesSecret(KafkaCluster cr, String secretName) {
        if (cr.getSpec().getApicurio() == null) return false;
        var storage = cr.getSpec().getApicurio().getStorage();
        return storage != null
                && secretName != null
                && secretName.equals(storage.getTlsSecretRef());
    }

    private ApicurioRegistry synthetic(KafkaCluster cr, String namespace,
                                       boolean mcsEnabled, List<String> targetClusters) {
        KafkaClusterApicurioSpec src = cr.getSpec().getApicurio();
        ApicurioRegistry syn = new ApicurioRegistry();
        syn.setMetadata(new ObjectMetaBuilder()
                .withName(APICURIO_NAME).withNamespace(namespace)
                .withOwnerReferences(clusterOwnerRef(cr))
                .build());
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        spec.setImage(src.getImage());
        spec.setRbacProxyImage(src.getRbacProxyImage());
        spec.setRbacRef(src.getRbacRef());
        spec.setReplicas(src.getReplicas());
        spec.setOidc(src.getOidc());
        // Storage: copy + populate clusterRef from the parent (the kafkasqlSupport reads it).
        if (src.getStorage() != null) {
            ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
            storage.setType(src.getStorage().getType());
            storage.setJdbcUrl(src.getStorage().getJdbcUrl());
            storage.setJdbcSecretRef(src.getStorage().getJdbcSecretRef());
            storage.setKafkaTopic(src.getStorage().getKafkaTopic());
            storage.setKafkaTopicPartitions(src.getStorage().getKafkaTopicPartitions());
            storage.setTlsSecretRef(src.getStorage().getTlsSecretRef());
            storage.setPrincipal(src.getStorage().getPrincipal());
            storage.setClusterRef(cr.getMetadata().getName());
            spec.setStorage(storage);
        }
        spec.setExternalAccess(src.getExternalAccess());
        if (mcsEnabled) {
            McsConfig mcs = new McsConfig();
            mcs.setEnabled(true);
            spec.setMcs(mcs);
            spec.setTargetClusters(targetClusters);
            spec.setExportService(true);
        }
        syn.setSpec(spec);
        return syn;
    }

    private List<OwnerReference> clusterOwnerRef(KafkaCluster cr) {
        return OwnerReferences.singleton(cr);
    }

    /** Apply or remove the Apicurio-metrics Service + ServiceMonitor based on
     *  {@code KafkaCluster.spec.metricsConfig}. Apicurio exposes {@code /metrics} on the
     *  registry HTTP port (8080), no separate JMX exporter required. */
    private void applyOrDeleteMetrics(KafkaCluster cr, String namespace) {
        String baseName = APICURIO_NAME;
        Map<String, String> labels = ApicurioDeploymentBuilder.labels(APICURIO_NAME);
        if (cr.getSpec().getMetricsConfig() == null) {
            client.services().inNamespace(namespace)
                  .withName(baseName + se.afshin.yavari.kafka.operator.infra.MetricsResources.METRICS_SUFFIX)
                  .delete();
            optionalApplier.deleteServiceMonitor(
                    baseName + se.afshin.yavari.kafka.operator.infra.MetricsResources.METRICS_SUFFIX, namespace);
            return;
        }
        List<OwnerReference> owner = clusterOwnerRef(cr);
        io.fabric8.kubernetes.api.model.Service svc = metricsResources.metricsService(
                baseName, namespace, labels, labels, "http",
                ApicurioDeploymentBuilder.REGISTRY_PORT, owner);
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        io.fabric8.kubernetes.api.model.GenericKubernetesResource sm = metricsResources.serviceMonitor(
                baseName, namespace, labels, labels, "http",
                se.afshin.yavari.kafka.operator.infra.MetricsResources.DEFAULT_INTERVAL,
                "/metrics", owner);
        optionalApplier.applyServiceMonitor(sm, namespace);
    }

    private void attachOwnerRef(Deployment dep, KafkaCluster cr) {
        if (dep.getMetadata() == null) return;
        if (dep.getMetadata().getOwnerReferences() == null
                || dep.getMetadata().getOwnerReferences().isEmpty()) {
            dep.getMetadata().setOwnerReferences(clusterOwnerRef(cr));
        }
    }

    private void attachOwnerRefSvc(Service svc, KafkaCluster cr) {
        if (svc.getMetadata() == null) return;
        if (svc.getMetadata().getOwnerReferences() == null
                || svc.getMetadata().getOwnerReferences().isEmpty()) {
            svc.getMetadata().setOwnerReferences(clusterOwnerRef(cr));
        }
    }

    private boolean isTls(HttpExternalAccessConfig ea) {
        if (ea == null) return false;
        if (ea.getIngress() != null && ea.getIngress().getTlsSecretRef() != null
                && !ea.getIngress().getTlsSecretRef().isBlank()) return true;
        if (ea.getGateway() != null && ea.getGateway().getTlsSecretRef() != null
                && !ea.getGateway().getTlsSecretRef().isBlank()) return true;
        return false;
    }

    private void applyOrDeleteIngress(ApicurioRegistry syn, HttpExternalAccessConfig ea,
                                      ExternalAccessResolution external) {
        String namespace = syn.getMetadata().getNamespace();
        String svcName = APICURIO_NAME + "-rbac-proxy";
        if (ea.getType() == ExternalAccessType.INGRESS && external.advertisedHost() != null) {
            Ingress ingress = httpIngressBuilder.build(svcName, namespace,
                    ApicurioDeploymentBuilder.labels(APICURIO_NAME), null, external.advertisedHost(),
                    svcName, ApicurioProxyContainerBuilder.PROXY_PORT, ea.getIngress());
            optionalApplier.applyIngress(ingress, namespace);
        } else {
            optionalApplier.deleteIngress(svcName, namespace);
        }
    }

    private void applyOrDeleteHttpRoute(ApicurioRegistry syn, HttpExternalAccessConfig ea,
                                        ExternalAccessResolution external) {
        String namespace = syn.getMetadata().getNamespace();
        String svcName = APICURIO_NAME + "-rbac-proxy";
        if (ea.getType() == ExternalAccessType.GATEWAY && external.advertisedHost() != null) {
            GenericKubernetesResource route = httpRouteBuilder.build(svcName, namespace,
                    ApicurioDeploymentBuilder.labels(APICURIO_NAME), null, external.advertisedHost(),
                    svcName, ApicurioProxyContainerBuilder.PROXY_PORT, ea.getGateway());
            optionalApplier.applyHttpRoute(route, namespace);
        } else {
            optionalApplier.deleteHttpRoute(svcName, namespace);
        }
    }

    private int readyReplicas(String deploymentName, String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace)
                .withName(deploymentName).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }

    static boolean rollWillHappen(Deployment existing, String desiredImage, String desiredHash) {
        if (existing == null) return true;
        if (existing.getSpec() == null || existing.getSpec().getTemplate() == null) return true;
        var podSpec = existing.getSpec().getTemplate().getSpec();
        // The "registry" container is the one whose image is driven by spec.apicurio.image.
        // Defaulted images (where spec.image is null) shouldn't roll on null≠"defaultImg" —
        // Objects.equals treats null inputs from the spec as "no change requested".
        String existingImage = (podSpec != null
                && podSpec.getContainers() != null
                && !podSpec.getContainers().isEmpty())
            ? podSpec.getContainers().get(0).getImage()
            : null;
        if (desiredImage != null && !Objects.equals(desiredImage, existingImage)) return true;
        var meta = existing.getSpec().getTemplate().getMetadata();
        String existingHash = (meta != null && meta.getAnnotations() != null)
            ? meta.getAnnotations().getOrDefault(ApicurioDeploymentBuilder.CONFIG_HASH_ANNOTATION, "")
            : "";
        return !desiredHash.equals(existingHash);
    }
}
