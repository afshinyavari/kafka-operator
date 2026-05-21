package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
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
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ControllerConfiguration
@ApplicationScoped
public class KafkaProxyReconciler implements Reconciler<KafkaProxy>,
        Cleaner<KafkaProxy>, EventSourceInitializer<KafkaProxy> {

    private static final Logger LOG = Logger.getLogger(KafkaProxyReconciler.class);

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

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<KafkaProxy> context) {
        var poolSource = new InformerEventSource<>(
                InformerConfiguration.from(KafkaNodePool.class, context)
                        .withSecondaryToPrimaryMapper(pool -> {
                            String ns = pool.getMetadata().getNamespace();
                            String poolName = pool.getMetadata().getName();
                            return context.getClient()
                                    .resources(KafkaProxy.class).inNamespace(ns).list().getItems().stream()
                                    .filter(p -> poolName.equals(p.getSpec().getPoolRef()))
                                    .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        var rbacSource = new InformerEventSource<>(
                InformerConfiguration.from(KafkaRbac.class, context)
                        .withSecondaryToPrimaryMapper(rbac -> {
                            String ns = rbac.getMetadata().getNamespace();
                            String rbacName = rbac.getMetadata().getName();
                            return context.getClient()
                                    .resources(KafkaProxy.class).inNamespace(ns).list().getItems().stream()
                                    .filter(p -> rbacName.equals(p.getSpec().getRbacRef()))
                                    .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        var apicurioSource = new InformerEventSource<>(
                InformerConfiguration.from(ApicurioRegistry.class, context)
                        .withSecondaryToPrimaryMapper(apr -> {
                            String ns = apr.getMetadata().getNamespace();
                            String aprName = apr.getMetadata().getName();
                            return context.getClient()
                                    .resources(KafkaProxy.class).inNamespace(ns).list().getItems().stream()
                                    .filter(p -> aprName.equals(p.getSpec().getApicurioRef()))
                                    .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        // Wake the reconciler when a Secret that a KafkaProxy CR references is rotated,
        // so the configHash flips and the Deployment rolls.
        var secretSource = new InformerEventSource<>(
                InformerConfiguration.from(Secret.class, context)
                        .withSecondaryToPrimaryMapper(secret -> {
                            String ns = secret.getMetadata().getNamespace();
                            String secretName = secret.getMetadata().getName();
                            return context.getClient()
                                    .resources(KafkaProxy.class).inNamespace(ns).list().getItems().stream()
                                    .filter(p -> referencesSecret(p, secretName))
                                    .map(p -> new ResourceID(p.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        return EventSourceInitializer.nameEventSources(poolSource, rbacSource, apicurioSource,
                secretSource);
    }

    /** True if the given KafkaProxy's client-cert or server-cert Secret refs match {@code secretName}.
     *  Defaults follow the same convention as {@link ProxyDeploymentBuilder}: {@code {name}-client-tls}
     *  and {@code {name}-server-tls}. */
    static boolean referencesSecret(KafkaProxy proxy, String secretName) {
        String name = proxy.getMetadata().getName();
        KafkaProxyTlsConfig tls = proxy.getSpec().getTls();
        String client = (tls != null && tls.getClientCertSecretRef() != null)
                ? tls.getClientCertSecretRef() : name + "-client-tls";
        String server = (tls != null && tls.getServerCertSecretRef() != null)
                ? tls.getServerCertSecretRef() : name + "-server-tls";
        return secretName.equals(client) || secretName.equals(server);
    }

    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy proxy, Context<KafkaProxy> context) {
        String name = proxy.getMetadata().getName();
        String namespace = proxy.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaProxy %s/%s", namespace, name);

        KafkaProxyStatus status = proxy.getStatus() != null ? proxy.getStatus() : new KafkaProxyStatus();
        status.setPhase(KafkaProxyStatus.Phase.RECONCILING);

        McsConfig mcsCfg = proxy.getSpec().getMcs();
        boolean mcsEnabled = mcsCfg != null && mcsCfg.isEnabled();
        List<String> targetClusters = proxy.getSpec().getTargetClusters();

        // Reject inconsistent intent: targetClusters only makes sense in MCS mode.
        if (!mcsEnabled && targetClusters != null && !targetClusters.isEmpty()) {
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage("spec.targetClusters is set but spec.mcs.enabled is false");
            proxy.setStatus(status);
            return UpdateControl.patchStatus(proxy);
        }

        // In MCS mode the same CR is applied to every cluster in the topology, but the proxy
        // only deploys on the clusters listed in spec.targetClusters. Other clusters reach SKIPPED
        // with no resources reconciled — same idempotent reconcile shape, different outcome.
        if (mcsEnabled) {
            if (targetClusters == null || targetClusters.isEmpty()) {
                status.setPhase(KafkaProxyStatus.Phase.FAILED);
                status.setMessage("spec.mcs.enabled requires spec.targetClusters to be non-empty");
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy);
            }
            if (!targetClusters.contains(localClusterId)) {
                LOG.infof("KafkaProxy %s/%s: cluster '%s' not in targetClusters %s — skipping",
                        namespace, name, localClusterId, targetClusters);
                status.setPhase(KafkaProxyStatus.Phase.SKIPPED);
                status.setMessage("Cluster '" + localClusterId + "' is not a target for this proxy");
                status.setReadyReplicas(0);
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy);
            }
        }

        // Resolve broker count + node id base.
        int brokerCount;
        int brokerNodeIdBase;
        if (mcsEnabled) {
            // The pool CR may not exist on this cluster (it lives only on the broker's home cluster
            // in MCS topologies). Derive both values from spec.brokerNodeIdRanges, which the user
            // must populate in MCS mode (validated below).
            List<BrokerNodeIdRange> ranges = proxy.getSpec().getBrokerNodeIdRanges();
            if (ranges == null || ranges.isEmpty()) {
                status.setPhase(KafkaProxyStatus.Phase.FAILED);
                status.setMessage("spec.mcs.enabled requires spec.brokerNodeIdRanges to be non-empty");
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy);
            }
            brokerCount = ranges.stream().mapToInt(r -> r.getEnd() - r.getStart() + 1).sum();
            brokerNodeIdBase = ranges.stream().mapToInt(BrokerNodeIdRange::getStart).min().orElse(0);
        } else {
            KafkaNodePool pool = client.resources(KafkaNodePool.class)
                    .inNamespace(namespace).withName(proxy.getSpec().getPoolRef()).get();
            if (pool == null) {
                status.setMessage("KafkaNodePool '" + proxy.getSpec().getPoolRef() + "' not found");
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
            }
            brokerCount = pool.getSpec().getReplicas();
            brokerNodeIdBase = resolveNodeIdBase(pool.getMetadata().getName(), namespace);
        }

        // Resolve the parent KafkaCluster to confirm spec.proxyMtls.enabled. The operator does
        // NOT sign certs; it only mounts pre-provisioned secrets (cert-manager / mcs-setup).
        KafkaCluster cluster = client.resources(KafkaCluster.class)
                .inNamespace(namespace).withName(proxy.getSpec().getClusterRef()).get();
        if (cluster == null) {
            status.setMessage("KafkaCluster '" + proxy.getSpec().getClusterRef() + "' not found");
            proxy.setStatus(status);
            return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
        }
        KafkaProxyMtlsConfig proxyMtls = cluster.getSpec().getProxyMtls();
        if (proxyMtls == null || !proxyMtls.isEnabled()) {
            status.setMessage("KafkaCluster '" + cluster.getMetadata().getName()
                    + "' spec.proxyMtls.enabled must be true");
            proxy.setStatus(status);
            return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
        }

        // Cross-reference: every targetClusters entry must name a real ClusterEntry on the
        // referenced KafkaCluster. Catches typos that would otherwise produce silent skips on
        // every operator.
        if (mcsEnabled && cluster.getSpec().getClusters() != null) {
            java.util.Set<String> knownIds = cluster.getSpec().getClusters().stream()
                    .map(se.afshin.yavari.kafka.operator.crd.ClusterEntry::getId)
                    .collect(Collectors.toSet());
            for (String t : targetClusters) {
                if (!knownIds.contains(t)) {
                    status.setPhase(KafkaProxyStatus.Phase.FAILED);
                    status.setMessage("spec.targetClusters contains unknown cluster id '" + t
                            + "' — not in KafkaCluster.spec.clusters " + knownIds);
                    proxy.setStatus(status);
                    return UpdateControl.patchStatus(proxy);
                }
            }
        }

        // Resolve proxy client + server cert secret names (spec overrides, else defaults).
        KafkaProxyTlsConfig tls = proxy.getSpec().getTls();
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
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(10));
            }
        }

        // Resolve external access (LB ingress / Gateway / Ingress hostname). The Service must
        // exist first for LB auto-resolve, but on the very first reconcile no Service is up yet —
        // we apply the Service further down, then a subsequent reconcile picks up the LB ingress.
        ExternalAccessResolution external;
        try {
            external = externalAccessResolver.resolve(proxy, localClusterId, namespace, client);
        } catch (IllegalStateException e) {
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
            proxy.setStatus(status);
            return UpdateControl.patchStatus(proxy);
        }

        try {

            // Generate and apply config ConfigMap
            String configYaml = configBuilder.build(proxy, brokerCount, brokerNodeIdBase, namespace, mcsEnabled, external);
            // Fold mounted Secret resourceVersions in so cert-manager rotations re-roll the proxy.
            String secretRevisions = secretRevisionTracker.revisionsOf(
                    List.of(clientCertSecret, serverCertSecret), namespace);
            String configHash = ConfigHasher.sha256(configYaml, secretRevisions);
            ConfigMap configMap = new ConfigMapBuilder()
                    .withNewMetadata()
                        .withName(name + "-config")
                        .withNamespace(namespace)
                    .endMetadata()
                    .withData(Map.of("config.yaml", configYaml))
                    .build();
            client.configMaps().inNamespace(namespace).resource(configMap).createOrReplace();

            // Cross-cluster roll gate: if applying this Deployment would change the running
            // image OR the generated Kroxylicious config, treat it as a roll and honour
            // spec.clusterRollOrder so cluster A finishes before cluster B begins.
            Deployment existing = client.apps().deployments()
                    .inNamespace(namespace).withName(name).get();
            if (existing != null
                    && cluster.getSpec().getClusterRollOrder() != null
                    && !cluster.getSpec().getClusterRollOrder().isEmpty()) {
                boolean rollIsRequired = rollWillHappen(existing, proxy.getSpec().getImage(), configHash);
                if (rollIsRequired
                        && !rollCoordinator.isMyTurnToRoll(cluster.getSpec(), localClusterId)) {
                    LOG.infof("KafkaProxy %s/%s: waiting for preceding cluster per clusterRollOrder %s",
                            namespace, name, cluster.getSpec().getClusterRollOrder());
                    status.setMessage("Waiting for preceding cluster per spec.clusterRollOrder "
                            + "before rolling proxy (image or config change)");
                    proxy.setStatus(status);
                    return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
                }
            }

            // Tell the local UpgradePhaseResource that a roll is imminent BEFORE we apply.
            // Kubernetes' default rolling-update surge keeps the old pod Available while
            // the new one starts, so the Deployment.status check alone can miss the entire
            // roll window on fast rolls. The tracker closes that race.
            if (rollWillHappen(existing, proxy.getSpec().getImage(), configHash)) {
                rollTracker.markRolling(namespace, name);
            }

            // Apply Deployment
            Deployment deployment = deploymentBuilder.build(proxy, namespace, configHash);
            client.apps().deployments().inNamespace(namespace).resource(deployment).serverSideApply();

            // Apply Service
            Service service = serviceBuilder.build(proxy, brokerCount, namespace, external);
            client.services().inNamespace(namespace).resource(service).serverSideApply();

            // Apply ServiceExport if MCS enabled
            if (mcsEnabled) {
                applyServiceExport(name, namespace);
            }

            // Apply Gateway-API TLSRoute when externalAccess.type=GATEWAY. The hostnames advertise
            // the proxy to clients via the Gateway, which does TLS SNI passthrough to the proxy's
            // single listen port (sniHostIdentifiesNode dispatches to the right broker).
            if (external.type() == se.afshin.yavari.kafka.operator.crd.ExternalAccessType.GATEWAY) {
                applyTlsRoute(proxy, brokerCount, namespace, external);
            }

            // Apply Ingress when externalAccess.type=INGRESS. Ingress controller must support
            // ssl-passthrough (nginx-ingress with --enable-ssl-passthrough).
            if (external.type() == se.afshin.yavari.kafka.operator.crd.ExternalAccessType.INGRESS) {
                applyIngress(proxy, brokerCount, namespace, external);
            }

            // If LB ingress not yet allocated, the config we just wrote uses the fallback
            // internal DNS. Reschedule so the next pass picks up the real LB hostname/IP.
            if (external.isPending()) {
                LOG.infof("KafkaProxy %s/%s: LoadBalancer ingress not yet assigned — rescheduling",
                        namespace, name);
                status.setMessage("Waiting for LoadBalancer ingress address...");
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
            }

            // Update status from Deployment readiness
            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= proxy.getSpec().getReplicas()) {
                status.setPhase(KafkaProxyStatus.Phase.READY);
                status.setMessage(null);
                // The previous roll (if any) is now visibly converged — clear the in-memory
                // marker so successor clusters polling our /operator/upgrade-phase get IDLE.
                rollTracker.markComplete(namespace, name);
            } else {
                status.setMessage("Waiting for proxy pods: " + ready + "/" + proxy.getSpec().getReplicas());
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
            }
        } catch (Exception e) {
            LOG.errorf("KafkaProxy %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaProxyStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        proxy.setStatus(status);
        return UpdateControl.patchStatus(proxy);
    }

    @Override
    public DeleteControl cleanup(KafkaProxy proxy, Context<KafkaProxy> context) {
        String namespace = proxy.getMetadata().getNamespace();
        String name = proxy.getMetadata().getName();
        LOG.infof("KafkaProxy %s deleted", name);
        client.configMaps().inNamespace(namespace).withName(name + "-config").delete();
        client.apps().deployments().inNamespace(namespace).withName(name).delete();
        client.services().inNamespace(namespace).withName(name).delete();
        // TLS secrets are NOT operator-owned (cert-manager / mcs-setup provisions them);
        // leave them in place on KafkaProxy delete.
        McsConfig mcsCfg = proxy.getSpec().getMcs();
        if (mcsCfg != null && mcsCfg.isEnabled()) {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                    .inNamespace(namespace).withName(name).delete();
        }
        // Best-effort TLSRoute cleanup. Mirrors the ServiceExport pattern: we don't know
        // whether the cluster has the Gateway API CRDs installed, so swallow CRD-missing errors.
        try {
            client.genericKubernetesResources(TLSRouteBuilder.API_VERSION, TLSRouteBuilder.KIND)
                    .inNamespace(namespace).withName(name).delete();
        } catch (Exception e) {
            LOG.warnf("TLSRoute cleanup skipped for %s/%s: %s", namespace, name, e.getMessage());
        }
        // Best-effort Ingress cleanup.
        try {
            client.network().v1().ingresses().inNamespace(namespace).withName(name).delete();
        } catch (Exception e) {
            LOG.warnf("Ingress cleanup skipped for %s/%s: %s", namespace, name, e.getMessage());
        }
        return DeleteControl.defaultDelete();
    }

    private void applyServiceExport(String serviceName, String namespace) {
        GenericKubernetesResource export = new GenericKubernetesResource();
        export.setApiVersion("multicluster.x-k8s.io/v1alpha1");
        export.setKind("ServiceExport");
        export.setMetadata(new ObjectMetaBuilder()
                .withName(serviceName)
                .withNamespace(namespace)
                .build());
        try {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                    .inNamespace(namespace).resource(export).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("ServiceExport CRD not available — skipped for %s: %s", serviceName, e.getMessage());
        }
    }

    private void applyTlsRoute(KafkaProxy proxy, int brokerCount, String namespace,
                                ExternalAccessResolution external) {
        try {
            GenericKubernetesResource route = tlsRouteBuilder.build(proxy, brokerCount, namespace, external);
            client.genericKubernetesResources(TLSRouteBuilder.API_VERSION, TLSRouteBuilder.KIND)
                    .inNamespace(namespace).resource(route).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("TLSRoute CRD not available — skipped for %s/%s: %s",
                    namespace, proxy.getMetadata().getName(), e.getMessage());
        }
    }

    private void applyIngress(KafkaProxy proxy, int brokerCount, String namespace,
                               ExternalAccessResolution external) {
        try {
            io.fabric8.kubernetes.api.model.networking.v1.Ingress ingress =
                    ingressBuilder.build(proxy, brokerCount, namespace, external);
            client.network().v1().ingresses().inNamespace(namespace)
                    .resource(ingress).serverSideApply();
        } catch (Exception e) {
            LOG.warnf("Ingress apply failed for %s/%s: %s",
                    namespace, proxy.getMetadata().getName(), e.getMessage());
        }
    }

    private int resolveNodeIdBase(String poolName, String namespace) {
        return client.pods().inNamespace(namespace)
                .withLabel("kafka.yavari.afshin.se/node-pool", poolName)
                .list().getItems().stream()
                .mapToInt(p -> {
                    String id = p.getMetadata().getLabels().get("kafka.yavari.afshin.se/node-id");
                    return id != null ? Integer.parseInt(id) : 0;
                })
                .min()
                .orElse(0);
    }

    /** Will applying a Deployment with the given desired image + config hash trigger an
     *  actual roll? Returns true if anything material differs (or there's no existing
     *  Deployment to compare against). Defensive against partially-populated Deployment
     *  objects from mocks or stale informers. */
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
