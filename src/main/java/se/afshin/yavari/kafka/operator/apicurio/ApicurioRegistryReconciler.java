package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
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
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;

import java.time.Duration;
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

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

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
        return EventSourceInitializer.nameEventSources(rbacEventSource);
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

        String rbacRef = registry.getSpec().getRbacRef();
        boolean proxyEnabled = rbacRef != null && registry.getSpec().getRbacProxyImage() != null;

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
            Container proxyContainer = null;
            Volume policyVolume = null;
            if (proxyEnabled) {
                proxyContainer = proxyContainerBuilder.build(registry);
                policyVolume = proxyContainerBuilder.policyVolume(rbacRef);
            }

            Deployment dep = deploymentBuilder.build(registry, namespace, proxyContainer, policyVolume);
            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            if (proxyEnabled) {
                Service proxySvc = proxyServiceBuilder.build(registry, namespace);
                client.services().inNamespace(namespace).resource(proxySvc).serverSideApply();
            }

            if (registry.getSpec().isExportService() && proxyEnabled) {
                applyServiceExport(name + "-rbac-proxy", namespace);
            }

            if (proxyEnabled) {
                String proxyUrl = "http://" + name + "-rbac-proxy." + namespace
                        + ".svc.cluster.local:" + ApicurioProxyContainerBuilder.PROXY_PORT;
                status.setProxyUrl(proxyUrl);
            }

            int ready = readyReplicas(name + "-registry", namespace);
            if (ready >= registry.getSpec().getReplicas()) {
                status.setPhase(ApicurioRegistryStatus.Phase.READY);
                status.setMessage(null);
            } else {
                status.setMessage("Waiting for registry pods: " + ready
                        + "/" + registry.getSpec().getReplicas());
                registry.setStatus(status);
                return UpdateControl.patchStatus(registry).rescheduleAfter(Duration.ofSeconds(15));
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
        LOG.infof("ApicurioRegistry %s deleted", name);
        client.apps().deployments().inNamespace(namespace).withName(name + "-registry").delete();
        client.services().inNamespace(namespace).withName(name + "-rbac-proxy").delete();
        if (mcsEnabled) {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                    .inNamespace(namespace).withName(name + "-rbac-proxy").delete();
        }
        return DeleteControl.defaultDelete();
    }

    private void applyServiceExport(String serviceName, String namespace) {
        if (!mcsEnabled) return;
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

    private int readyReplicas(String deploymentName, String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace)
                .withName(deploymentName).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }
}
