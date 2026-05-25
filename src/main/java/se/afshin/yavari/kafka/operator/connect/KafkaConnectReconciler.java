package se.afshin.yavari.kafka.operator.connect;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaConnect;
import se.afshin.yavari.kafka.operator.crd.KafkaConnectStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaEndpoint;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;
import se.afshin.yavari.kafka.operator.endpoint.KafkaEndpointResolver;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.McsPlacement;
import se.afshin.yavari.kafka.operator.infra.MetricsResources;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.ReconcileContext;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.infra.ServiceExportManager;
import se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reconciler for the {@link KafkaConnect} CRD. Provisions a distributed-mode Connect
 * worker cluster attached to a single Kafka cluster. Mirrors the {@code MirrorMaker2Reconciler}
 * shape — orchestrator + builders (CLAUDE.md rule: reconcilers stay thin).
 *
 * <p>Flow per reconcile pass:
 * <ol>
 *   <li>MCS placement gate.
 *   <li>Validate spec (kafkaClusterRef set; no Confluent registry in v1).
 *   <li>groupId collision guard.
 *   <li>Resolve the Kafka endpoint.
 *   <li>Resolve plugin sources (PVC + ConfigMaps + Secrets — must exist).
 *   <li>Render properties + apply ConfigMap (with configHash folding in Secret revisions).
 *   <li>Apply 3 internal-topic CRs (when attached cluster is managed).
 *   <li>Apply the Deployment.
 *   <li>Apply the REST Service.
 *   <li>Apply metrics resources (when {@code metricsConfig} set).
 *   <li>Apply ServiceExport (MCS).
 *   <li>Reschedule until {@code readyReplicas} catches up.
 * </ol>
 */
@ControllerConfiguration
@ApplicationScoped
public class KafkaConnectReconciler implements Reconciler<KafkaConnect>, Cleaner<KafkaConnect> {

    private static final Logger LOG = Logger.getLogger(KafkaConnectReconciler.class);

    @Inject KubernetesClient client;
    @Inject KafkaEndpointResolver endpointResolver;
    @Inject ConnectConfigBuilder configBuilder;
    @Inject ConnectConfigMapBuilder configMapBuilder;
    @Inject ConnectDeploymentBuilder deploymentBuilder;
    @Inject ConnectRestServiceBuilder restServiceBuilder;
    @Inject ConnectInternalTopicsBuilder internalTopicsBuilder;
    @Inject ConnectPluginResolver pluginResolver;
    @Inject ConnectGroupIdGuard groupIdGuard;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject CrossClusterRollCoordinator rollCoordinator;
    @Inject MetricsResources metricsResources;
    @Inject OptionalResourceApplier optionalApplier;
    @Inject se.afshin.yavari.kafka.operator.infra.PdbBuilder pdbBuilder;

    @ConfigProperty(name = "kafka.cluster.id", defaultValue = "")
    String localClusterId;

    @Override
    public UpdateControl<KafkaConnect> reconcile(KafkaConnect cr, Context<KafkaConnect> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<KafkaConnect> reconcileInner(KafkaConnect cr) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaConnect %s/%s", namespace, name);

        KafkaConnectStatus status = cr.getStatus() != null ? cr.getStatus() : new KafkaConnectStatus();
        status.setPhase(KafkaConnectStatus.Phase.RECONCILING);
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        // MCS placement gate
        McsConfig mcs = cr.getSpec().getMcs();
        List<String> targetClusters = cr.getSpec().getTargetClusters();
        boolean mcsEnabled = mcs != null && mcs.isEnabled();
        if (McsPlacement.decide(mcs, targetClusters, localClusterId) == McsPlacement.Decision.SKIP) {
            LOG.infof("KafkaConnect %s/%s: cluster '%s' not in targetClusters %s — skipping",
                    namespace, name, localClusterId, targetClusters);
            status.setPhase(KafkaConnectStatus.Phase.SKIPPED);
            status.setMessage("Cluster '" + localClusterId + "' is not a target for this KafkaConnect");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        String validationError = validate(cr);
        if (validationError != null) {
            status.setPhase(KafkaConnectStatus.Phase.FAILED);
            status.setMessage(validationError);
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        // groupId collision guard
        ConnectGroupIdGuard.Conflict conflict = groupIdGuard.check(cr);
        if (conflict != null) {
            status.setPhase(KafkaConnectStatus.Phase.FAILED);
            status.setMessage("groupId '" + cr.resolvedGroupId() + "' conflicts with KafkaConnect '"
                    + conflict.otherCrName() + "' (same Kafka cluster). Set a unique spec.groupId.");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        OwnerReference ownerRef = new OwnerReferenceBuilder()
                .withApiVersion(cr.getApiVersion())
                .withKind(cr.getKind())
                .withName(name)
                .withUid(cr.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();

        try {
            ResolvedKafkaEndpoint endpoint = endpointResolver.resolve(
                    cr.getSpec().getKafkaClusterRef(), namespace);
            status.setBootstrap(endpoint.bootstrap());

            ResolvedPluginSources plugins = pluginResolver.resolve(
                    cr.getSpec().getPluginSources(), namespace);
            status.setPluginPath(plugins.pluginPath());

            int replicas = effectiveReplicas(cr, endpoint);

            // Internal topics on the attached managed cluster (if any).
            //
            // Apply the CRs and gate Deployment creation on them all reaching READY: Connect's
            // worker auto-creates internal topics on startup with broker-default config, which
            // races our KafkaTopic reconciler and can leave the topics with wrong RF or
            // cleanup.policy. Waiting closes the race — KafkaTopicReconciler creates the
            // topics with the right config, then we boot Connect.
            if (cr.getSpec().getKafkaClusterRef().hasManaged()) {
                String targetClusterName = cr.getSpec().getKafkaClusterRef().getKafkaClusterRef().getName();
                List<KafkaTopic> topics = internalTopicsBuilder.build(cr, targetClusterName, ownerRef);
                for (KafkaTopic t : topics) {
                    client.resources(KafkaTopic.class).inNamespace(namespace)
                            .resource(t).serverSideApply();
                }
                // Gate: wait until all 3 internal topics report status.phase=READY.
                List<String> notReady = new ArrayList<>();
                for (KafkaTopic t : topics) {
                    KafkaTopic latest = client.resources(KafkaTopic.class).inNamespace(namespace)
                            .withName(t.getMetadata().getName()).get();
                    if (latest == null
                            || latest.getStatus() == null
                            || latest.getStatus().getPhase() != se.afshin.yavari.kafka.operator.crd.KafkaTopicStatus.Phase.READY) {
                        notReady.add(t.getMetadata().getName());
                    }
                }
                if (!notReady.isEmpty()) {
                    status.setPhase(KafkaConnectStatus.Phase.PENDING);
                    status.setMessage("Waiting for internal topics to reach READY: " + notReady);
                    cr.setStatus(status);
                    return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(5));
                }
            }

            String properties = configBuilder.build(cr, endpoint, plugins);

            // Fold endpoint Secrets + plugin Secrets into the hash so any rotation rolls.
            List<String> trackedSecrets = new ArrayList<>();
            if (endpoint.tlsSecretRef() != null) trackedSecrets.add(endpoint.tlsSecretRef());
            if (endpoint.hasSasl()) trackedSecrets.add(endpoint.sasl().secretRef());
            trackedSecrets.addAll(plugins.referencedSecretNames());
            String secretRevisions = secretRevisionTracker.revisionsOf(trackedSecrets, namespace);

            boolean metricsEnabled = cr.getSpec().getMetricsConfig() != null;
            String jmxConfigYaml = metricsEnabled
                    ? MetricsResources.jmxConfig("connect-jmx-config.yaml") : null;

            String configHash = ConfigHasher.sha256(properties, secretRevisions,
                    jmxConfigYaml == null ? "" : jmxConfigYaml,
                    plugins.pluginPath());

            ConfigMap cm = configMapBuilder.build(cr, properties, jmxConfigYaml, ownerRef);
            client.configMaps().inNamespace(namespace).resource(cm).serverSideApply();

            Deployment dep = deploymentBuilder.build(cr, endpoint, plugins, replicas, configHash,
                    metricsEnabled, ownerRef);

            // Cross-cluster roll gate.
            Deployment existing = client.apps().deployments().inNamespace(namespace).withName(name).get();
            if (existing != null && cr.getSpec().getClusterRollOrder() != null
                    && !cr.getSpec().getClusterRollOrder().isEmpty()) {
                boolean rollIsRequired = rollWillHappen(existing, cr.getSpec().getImage(), configHash);
                var pseudoSpec = new se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec();
                pseudoSpec.setClusterRollOrder(cr.getSpec().getClusterRollOrder());
                pseudoSpec.setClusters(new ArrayList<>());
                if (rollIsRequired && !rollCoordinator.isMyTurnToRoll(pseudoSpec, localClusterId)) {
                    LOG.infof("KafkaConnect %s/%s: waiting for preceding cluster per clusterRollOrder %s",
                            namespace, name, cr.getSpec().getClusterRollOrder());
                    status.setMessage("Waiting for preceding cluster per spec.clusterRollOrder");
                    cr.setStatus(status);
                    return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
                }
            }

            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            Service restService = restServiceBuilder.build(cr, ownerRef);
            client.services().inNamespace(namespace).resource(restService).serverSideApply();
            status.setUrl("http://" + restService.getMetadata().getName() + "." + namespace
                    + ".svc.cluster.local:" + cr.getSpec().getRestPort());

            if (metricsEnabled) {
                applyMetrics(name, namespace, ownerRef);
            } else {
                deleteMetrics(name, namespace);
            }

            pdbBuilder.apply(name, namespace,
                    ConnectLabels.labels(name), ConnectLabels.labels(name),
                    replicas, cr);

            if (mcsEnabled) {
                serviceExportManager.apply(restService.getMetadata().getName(), namespace);
                if (metricsEnabled) {
                    serviceExportManager.apply(name + MetricsResources.METRICS_SUFFIX, namespace);
                }
            }

            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= replicas) {
                status.setPhase(KafkaConnectStatus.Phase.READY);
                status.setMessage(null);
            } else {
                status.setPhase(KafkaConnectStatus.Phase.PENDING);
                status.setMessage("Waiting for Connect workers: " + ready + "/" + replicas);
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
            }
        } catch (Exception e) {
            LOG.errorf("KafkaConnect %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaConnectStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        cr.setStatus(status);
        return UpdateControl.patchStatus(cr);
    }

    @Override
    public DeleteControl cleanup(KafkaConnect cr, Context<KafkaConnect> ctx) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        McsConfig mcs = cr.getSpec().getMcs();
        if (mcs != null && mcs.isEnabled()) {
            serviceExportManager.delete(name + ConnectRestServiceBuilder.SUFFIX, namespace);
            serviceExportManager.delete(name + MetricsResources.METRICS_SUFFIX, namespace);
        }
        deleteMetrics(name, namespace);
        pdbBuilder.delete(name, namespace);
        LOG.infof("KafkaConnect %s/%s deleted", namespace, name);
        return DeleteControl.defaultDelete();
    }

    private void applyMetrics(String name, String namespace, OwnerReference ownerRef) {
        Map<String, String> labels = ConnectLabels.labels(name);
        Service svc = metricsResources.metricsService(name, namespace, labels, labels,
                "metrics", ConnectDeploymentBuilder.METRICS_PORT, List.of(ownerRef));
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        GenericKubernetesResource sm = metricsResources.serviceMonitor(name, namespace,
                labels, labels, "metrics", List.of(ownerRef));
        optionalApplier.applyServiceMonitor(sm, namespace);
    }

    private void deleteMetrics(String name, String namespace) {
        client.services().inNamespace(namespace)
              .withName(name + MetricsResources.METRICS_SUFFIX).delete();
        optionalApplier.deleteServiceMonitor(name + MetricsResources.METRICS_SUFFIX, namespace);
    }

    /** Default replicas: 3 when the attached cluster is multi-cluster managed, else 1.
     *  User-supplied value wins. */
    int effectiveReplicas(KafkaConnect cr, ResolvedKafkaEndpoint endpoint) {
        if (cr.getSpec().getReplicas() != null && cr.getSpec().getReplicas() > 0) {
            return cr.getSpec().getReplicas();
        }
        if (cr.getSpec().getKafkaClusterRef().hasManaged()) {
            KafkaCluster c = client.resources(KafkaCluster.class)
                    .inNamespace(cr.getMetadata().getNamespace())
                    .withName(cr.getSpec().getKafkaClusterRef().getKafkaClusterRef().getName()).get();
            if (c != null && c.getSpec().getClusters() != null
                    && c.getSpec().getClusters().size() > 1) {
                return 3;
            }
        }
        return 1;
    }

    private String validate(KafkaConnect cr) {
        if (cr.getSpec().getKafkaClusterRef() == null) return "spec.kafkaClusterRef is required";
        KafkaEndpoint ep = cr.getSpec().getKafkaClusterRef();
        if (!ep.hasManaged() && !ep.hasExternal()) {
            return "spec.kafkaClusterRef must reference a managed cluster or an external endpoint";
        }
        if (ep.hasExternal() && ep.getExternal().getSchemaRegistry() != null
                && ep.getExternal().getSchemaRegistry().getType() == SchemaRegistryType.CONFLUENT) {
            return "spec.kafkaClusterRef: schemaRegistry.type=CONFLUENT is not supported in v1";
        }
        return null;
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
        if (existing == null || existing.getSpec() == null
                || existing.getSpec().getTemplate() == null) return true;
        var podSpec = existing.getSpec().getTemplate().getSpec();
        String existingImage = (podSpec != null
                && podSpec.getContainers() != null && !podSpec.getContainers().isEmpty())
                ? podSpec.getContainers().get(0).getImage() : null;
        if (desiredImage != null && !desiredImage.equals(existingImage)) return true;
        var meta = existing.getSpec().getTemplate().getMetadata();
        String existingHash = (meta != null && meta.getAnnotations() != null)
                ? meta.getAnnotations().getOrDefault(ConnectDeploymentBuilder.CONFIG_HASH_ANNOTATION, "")
                : "";
        return !desiredHash.equals(existingHash);
    }
}
