package se.afshin.yavari.kafka.operator.reconciler;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
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
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.proxy.KroxyliciousConfigBuilder;
import se.afshin.yavari.kafka.operator.proxy.ProxyDeploymentBuilder;
import se.afshin.yavari.kafka.operator.proxy.ProxyServiceBuilder;

import java.time.Duration;
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

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

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

        return EventSourceInitializer.nameEventSources(poolSource, rbacSource, apicurioSource);
    }

    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy proxy, Context<KafkaProxy> context) {
        String name = proxy.getMetadata().getName();
        String namespace = proxy.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaProxy %s/%s", namespace, name);

        KafkaProxyStatus status = proxy.getStatus() != null ? proxy.getStatus() : new KafkaProxyStatus();
        status.setPhase(KafkaProxyStatus.Phase.RECONCILING);

        // Resolve KafkaNodePool for broker count
        KafkaNodePool pool = client.resources(KafkaNodePool.class)
                .inNamespace(namespace).withName(proxy.getSpec().getPoolRef()).get();
        if (pool == null) {
            status.setMessage("KafkaNodePool '" + proxy.getSpec().getPoolRef() + "' not found");
            proxy.setStatus(status);
            return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(15));
        }
        int brokerCount = pool.getSpec().getReplicas();
        int brokerNodeIdBase = resolveNodeIdBase(pool.getMetadata().getName(), namespace);

        // Resolve Apicurio registry URL if referenced
        String apicurioRegistryUrl = null;
        String apicurioRef = proxy.getSpec().getApicurioRef();
        if (apicurioRef != null) {
            ApicurioRegistry apicurio = client.resources(ApicurioRegistry.class)
                    .inNamespace(namespace).withName(apicurioRef).get();
            if (apicurio != null && apicurio.getStatus() != null) {
                apicurioRegistryUrl = apicurio.getStatus().getRegistryUrl();
            }
            if (apicurioRegistryUrl == null) {
                status.setMessage("Waiting for ApicurioRegistry '" + apicurioRef + "' to be ready");
                proxy.setStatus(status);
                return UpdateControl.patchStatus(proxy).rescheduleAfter(Duration.ofSeconds(10));
            }
        }

        try {
            // Generate and apply config ConfigMap
            String configYaml = configBuilder.build(proxy, brokerCount, brokerNodeIdBase, apicurioRegistryUrl, namespace);
            ConfigMap configMap = new ConfigMapBuilder()
                    .withNewMetadata()
                        .withName(name + "-config")
                        .withNamespace(namespace)
                    .endMetadata()
                    .withData(Map.of("config.yaml", configYaml))
                    .build();
            client.configMaps().inNamespace(namespace).resource(configMap).serverSideApply();

            // Apply Deployment
            Deployment deployment = deploymentBuilder.build(proxy, namespace);
            client.apps().deployments().inNamespace(namespace).resource(deployment).serverSideApply();

            // Apply Service
            Service service = serviceBuilder.build(proxy, brokerCount, namespace);
            client.services().inNamespace(namespace).resource(service).serverSideApply();

            // Apply ServiceExport if MCS enabled
            if (mcsEnabled) {
                applyServiceExport(name, namespace);
            }

            // Update status from Deployment readiness
            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= proxy.getSpec().getReplicas()) {
                status.setPhase(KafkaProxyStatus.Phase.READY);
                status.setMessage(null);
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
        if (mcsEnabled) {
            client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport")
                    .inNamespace(namespace).withName(name).delete();
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

    private int readyReplicas(String deploymentName, String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace)
                .withName(deploymentName).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }
}
