package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
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
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressBuilder;
import se.afshin.yavari.kafka.operator.externalaccess.HttpRouteBuilder;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolution;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ControllerConfiguration
@ApplicationScoped
public class ApicurioRegistryReconciler implements Reconciler<ApicurioRegistry>,
        Cleaner<ApicurioRegistry>, EventSourceInitializer<ApicurioRegistry> {

    private static final Logger LOG = Logger.getLogger(ApicurioRegistryReconciler.class);

    @Inject KubernetesClient client;
    @Inject ApicurioDeploymentBuilder deploymentBuilder;
    @Inject ApicurioProxyContainerBuilder proxyContainerBuilder;
    @Inject ApicurioProxyServiceBuilder proxyServiceBuilder;
    @Inject ApicurioKafkasqlSupport kafkasqlSupport;
    @Inject ExternalAccessResolver externalAccessResolver;
    @Inject HttpIngressBuilder httpIngressBuilder;
    @Inject HttpRouteBuilder httpRouteBuilder;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject OptionalResourceApplier optionalApplier;

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    @ConfigProperty(name = "kafka.cluster.id", defaultValue = "")
    String localClusterId;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<ApicurioRegistry> context) {
        var rbacEventSource = new InformerEventSource<>(
                InformerConfiguration.from(KafkaRbac.class, context)
                        .withSecondaryToPrimaryMapper(rbac -> {
                            String ns = rbac.getMetadata().getNamespace();
                            String rbacName = rbac.getMetadata().getName();
                            return context.getClient()
                                    .resources(ApicurioRegistry.class)
                                    .inNamespace(ns).list().getItems().stream()
                                    .filter(r -> rbacName.equals(r.getSpec().getRbacRef()))
                                    .map(r -> new ResourceID(r.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        // Wake the reconciler when the kafkasql TLS Secret (the only Secret the registry pod
        // mounts) is rotated, so configHash flips and the Deployment rolls.
        var secretEventSource = new InformerEventSource<>(
                InformerConfiguration.from(Secret.class, context)
                        .withSecondaryToPrimaryMapper(secret -> {
                            String ns = secret.getMetadata().getNamespace();
                            String secretName = secret.getMetadata().getName();
                            return context.getClient()
                                    .resources(ApicurioRegistry.class)
                                    .inNamespace(ns).list().getItems().stream()
                                    .filter(r -> referencesSecret(r, secretName))
                                    .map(r -> new ResourceID(r.getMetadata().getName(), ns))
                                    .collect(Collectors.toSet());
                        })
                        .build(),
                context);

        return EventSourceInitializer.nameEventSources(rbacEventSource, secretEventSource);
    }

    /** True if the registry's kafkasql storage points at the given Secret. */
    static boolean referencesSecret(ApicurioRegistry registry, String secretName) {
        var storage = registry.getSpec().getStorage();
        return storage != null
                && secretName != null
                && secretName.equals(storage.getTlsSecretRef());
    }

    @Override
    public UpdateControl<ApicurioRegistry> reconcile(ApicurioRegistry registry,
                                                      Context<ApicurioRegistry> context) {
        String name = registry.getMetadata().getName();
        String namespace = registry.getMetadata().getNamespace();
        LOG.infof("Reconciling ApicurioRegistry %s/%s", namespace, name);

        ApicurioRegistryStatus status = registry.getStatus() != null
                ? registry.getStatus() : new ApicurioRegistryStatus();
        status.setPhase(ApicurioRegistryStatus.Phase.RECONCILING);

        McsConfig mcsCfg = registry.getSpec().getMcs();
        boolean specMcsEnabled = mcsCfg != null && mcsCfg.isEnabled();
        List<String> targetClusters = registry.getSpec().getTargetClusters();

        if (!specMcsEnabled && targetClusters != null && !targetClusters.isEmpty()) {
            status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
            status.setMessage("spec.targetClusters is set but spec.mcs.enabled is false");
            registry.setStatus(status);
            return UpdateControl.patchStatus(registry);
        }

        if (specMcsEnabled) {
            if (targetClusters == null || targetClusters.isEmpty()) {
                status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
                status.setMessage("spec.mcs.enabled requires spec.targetClusters to be non-empty");
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry);
            }
            if (!targetClusters.contains(localClusterId)) {
                LOG.infof("ApicurioRegistry %s/%s: cluster '%s' not in targetClusters %s — skipping",
                        namespace, name, localClusterId, targetClusters);
                status.setPhase(ApicurioRegistryStatus.Phase.SKIPPED);
                status.setMessage("Cluster '" + localClusterId + "' is not a target for this registry");
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry);
            }
        }

        String rbacRef = registry.getSpec().getRbacRef();
        boolean proxyEnabled = rbacRef != null && registry.getSpec().getRbacProxyImage() != null;
        HttpExternalAccessConfig ea = registry.getSpec().getExternalAccess();

        // External access can only safely expose the OIDC-protected rbac-proxy. Reject otherwise.
        if (ea != null && ea.getType() != null && !proxyEnabled) {
            status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
            status.setMessage("externalAccess requires spec.rbacRef + spec.rbacProxyImage — only "
                    + "the rbac-proxy is safe to expose externally");
            registry.setStatus(status);
            return UpdateControl.patchStatus(registry);
        }

        if (rbacRef != null) {
            ConfigMap policyMap = client.configMaps().inNamespace(namespace)
                    .withName(rbacRef + "-apicurio-policy").get();
            if (policyMap == null) {
                status.setMessage("Waiting for KafkaRbac '" + rbacRef + "' to be reconciled");
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry).rescheduleAfter(Duration.ofSeconds(10));
            }
        }

        try {
            ApicurioDeploymentBuilder.KafkasqlConfig kafkasqlConfig = null;
            if (registry.getSpec().getStorage() != null
                    && "kafkasql".equals(registry.getSpec().getStorage().getType())) {
                ApicurioKafkasqlSupport.Result result = kafkasqlSupport.prepare(registry);
                if (result instanceof ApicurioKafkasqlSupport.Result.Failed f) {
                    status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
                    status.setMessage(f.message());
                    registry.setStatus(status);
                    return UpdateControl.patchStatus(registry);
                }
                if (result instanceof ApicurioKafkasqlSupport.Result.Pending p) {
                    status.setMessage(p.reason());
                    registry.setStatus(status);
                    return UpdateControl.patchStatus(registry).rescheduleAfter(Duration.ofSeconds(10));
                }
                kafkasqlConfig = ((ApicurioKafkasqlSupport.Result.Ready) result).config();
            }

            Container proxyContainer = null;
            Volume policyVolume = null;
            if (proxyEnabled) {
                proxyContainer = proxyContainerBuilder.build(registry);
                policyVolume = proxyContainerBuilder.policyVolume(rbacRef);
            }

            // Fold mounted Secret resourceVersions into a configHash so cert-manager
            // rotations re-roll the registry. The only Secret mounted into the registry pod
            // is the kafkasql client TLS secret (mTLS path). The rbac-proxy sidecar doesn't
            // mount Secrets, so no extra inputs are needed for it.
            String secretRevisions = "";
            if (kafkasqlConfig != null && kafkasqlConfig.tlsSecretRef() != null) {
                secretRevisions = secretRevisionTracker.revisionsOf(
                        List.of(kafkasqlConfig.tlsSecretRef()), namespace);
            }
            String configHash = ConfigHasher.sha256(secretRevisions);
            Deployment dep = deploymentBuilder.build(registry, namespace, proxyContainer,
                    policyVolume, kafkasqlConfig, configHash);
            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            String proxySvcName = name + "-rbac-proxy";
            if (proxyEnabled) {
                Service proxySvc = proxyServiceBuilder.build(registry, namespace);
                client.services().inNamespace(namespace).resource(proxySvc).serverSideApply();
            }

            // When MCS is enabled the same CR is applied to every cluster; each operator must
            // export its local rbac-proxy Service so cross-cluster clients can resolve
            // `apicurio-rbac-proxy.<ns>.svc.clusterset.local` and fan out via Submariner Lighthouse.
            boolean wantExport = proxyEnabled
                    && (specMcsEnabled || registry.getSpec().isExportService());
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
                    registry.setStatus(status);
                    return UpdateControl.patchStatus(registry);
                }
                applyOrDeleteIngress(registry, ea, external);
                applyOrDeleteHttpRoute(registry, ea, external);
                if (external.advertisedHost() != null) {
                    String scheme = isTls(ea) ? "https" : "http";
                    int port = scheme.equals("https") ? 443 : ApicurioProxyContainerBuilder.PROXY_PORT;
                    String hostPort = (ea.getType() == ExternalAccessType.LOADBALANCER)
                            ? external.advertisedHost() + ":" + ApicurioProxyContainerBuilder.PROXY_PORT
                            : external.advertisedHost() + (port == 80 || port == 443 ? "" : ":" + port);
                    status.setExternalUrl(scheme + "://" + hostPort);
                }
            } else {
                // No externalAccess configured; ensure stale Ingress/HTTPRoute are pruned.
                optionalApplier.deleteIngress(proxySvcName, namespace);
                optionalApplier.deleteHttpRoute(proxySvcName, namespace);
                status.setExternalUrl(null);
            }

            if (proxyEnabled) {
                String proxyUrl = "http://" + proxySvcName + "." + namespace
                        + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
                status.setProxyUrl(proxyUrl);
            }

            // LB pending doesn't block READY — the registry is fully functional internally;
            // only external clients are affected. We reschedule a follow-up so externalUrl
            // gets populated as soon as the LB ingress IP/hostname is allocated.
            boolean rescheduleForLb = external.isPending();
            if (rescheduleForLb) {
                status.setMessage("Registry ready; waiting for LoadBalancer ingress address for external access");
            }

            int ready = readyReplicas(name + "-registry", namespace);
            if (ready >= registry.getSpec().getReplicas()) {
                status.setPhase(ApicurioRegistryStatus.Phase.READY);
                if (!rescheduleForLb) status.setMessage(null);
            } else {
                status.setMessage("Waiting for registry pods: " + ready
                        + "/" + registry.getSpec().getReplicas());
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry).rescheduleAfter(Duration.ofSeconds(15));
            }
            if (rescheduleForLb) {
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry).rescheduleAfter(Duration.ofSeconds(5));
            }
        } catch (Exception e) {
            LOG.errorf("ApicurioRegistry %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(ApicurioRegistryStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        registry.setStatus(status);
        return UpdateControl.patchStatus(registry);
    }

    @Override
    public DeleteControl cleanup(ApicurioRegistry registry, Context<ApicurioRegistry> context) {
        String namespace = registry.getMetadata().getNamespace();
        String name = registry.getMetadata().getName();
        String proxySvcName = name + "-rbac-proxy";
        LOG.infof("ApicurioRegistry %s deleted", name);
        kafkasqlSupport.cleanup(registry);
        client.apps().deployments().inNamespace(namespace).withName(name + "-registry").delete();
        client.services().inNamespace(namespace).withName(proxySvcName).delete();
        optionalApplier.deleteIngress(proxySvcName, namespace);
        optionalApplier.deleteHttpRoute(proxySvcName, namespace);
        if (mcsEnabled) {
            serviceExportManager.delete(proxySvcName, namespace);
        }
        return DeleteControl.defaultDelete();
    }

    private boolean isTls(HttpExternalAccessConfig ea) {
        if (ea == null) return false;
        if (ea.getIngress() != null && ea.getIngress().getTlsSecretRef() != null
                && !ea.getIngress().getTlsSecretRef().isBlank()) {
            return true;
        }
        if (ea.getGateway() != null && ea.getGateway().getTlsSecretRef() != null
                && !ea.getGateway().getTlsSecretRef().isBlank()) {
            return true;
        }
        return false;
    }

    private void applyOrDeleteIngress(ApicurioRegistry registry, HttpExternalAccessConfig ea,
                                      ExternalAccessResolution external) {
        String name = registry.getMetadata().getName();
        String namespace = registry.getMetadata().getNamespace();
        String svcName = name + "-rbac-proxy";
        if (ea.getType() == ExternalAccessType.INGRESS && external.advertisedHost() != null) {
            Ingress ingress = httpIngressBuilder.build(svcName, namespace,
                    ApicurioDeploymentBuilder.labels(name), null, external.advertisedHost(),
                    svcName, ApicurioProxyContainerBuilder.PROXY_PORT, ea.getIngress());
            optionalApplier.applyIngress(ingress, namespace);
        } else {
            optionalApplier.deleteIngress(svcName, namespace);
        }
    }

    private void applyOrDeleteHttpRoute(ApicurioRegistry registry, HttpExternalAccessConfig ea,
                                        ExternalAccessResolution external) {
        String name = registry.getMetadata().getName();
        String namespace = registry.getMetadata().getNamespace();
        String svcName = name + "-rbac-proxy";
        if (ea.getType() == ExternalAccessType.GATEWAY && external.advertisedHost() != null) {
            GenericKubernetesResource route = httpRouteBuilder.build(svcName, namespace,
                    ApicurioDeploymentBuilder.labels(name), null, external.advertisedHost(),
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
}
