package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
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
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Status;
import se.afshin.yavari.kafka.operator.crd.Mm2Endpoint;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
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
 * Reconciler for the {@link MirrorMaker2} CRD. Mirrors {@code KafkaUIReconciler}'s
 * orchestrator + builders shape (CLAUDE.md rule: keep reconcilers thin).
 *
 * <p>Flow per reconcile pass:
 * <ol>
 *   <li>MCS placement gate — skip if {@code spec.mcs.enabled} and the local cluster
 *       isn't in {@code spec.targetClusters}.
 *   <li>Validate spec (at-least-one-managed-end, no Confluent in v1, both ends populated).
 *   <li>Resolve source + target endpoints via {@link Mm2EndpointResolver}.
 *   <li>Render the MM2 properties file via {@link Mm2ConfigBuilder}.
 *   <li>Apply 3 KafkaTopic CRs (internal topics) on the target managed cluster.
 *   <li>Apply the ConfigMap (with configHash) and Deployment.
 *   <li>Reschedule until {@code Deployment.status.readyReplicas} catches up.
 * </ol>
 *
 * <p>Status is reported as a high-level phase + readyReplicas. Per-connector telemetry
 * via the {@code mm2-status.{flow}} topic is deferred (TODO in {@link Mm2StatusReader}).
 */
@ControllerConfiguration
@ApplicationScoped
public class MirrorMaker2Reconciler implements Reconciler<MirrorMaker2>, Cleaner<MirrorMaker2> {

    private static final Logger LOG = Logger.getLogger(MirrorMaker2Reconciler.class);

    @Inject KubernetesClient client;
    @Inject Mm2EndpointResolver endpointResolver;
    @Inject Mm2ConfigBuilder configBuilder;
    @Inject Mm2ConfigMapBuilder configMapBuilder;
    @Inject Mm2DeploymentBuilder deploymentBuilder;
    @Inject Mm2InternalTopicsBuilder internalTopicsBuilder;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject ServiceExportManager serviceExportManager;
    @Inject CrossClusterRollCoordinator rollCoordinator;
    @Inject MetricsResources metricsResources;
    @Inject OptionalResourceApplier optionalApplier;

    @ConfigProperty(name = "kafka.cluster.id", defaultValue = "")
    String localClusterId;

    @Override
    public UpdateControl<MirrorMaker2> reconcile(MirrorMaker2 cr, Context<MirrorMaker2> ctx) {
        try (var ignored = ReconcileContext.scope(cr)) {
            return reconcileInner(cr);
        }
    }

    private UpdateControl<MirrorMaker2> reconcileInner(MirrorMaker2 cr) {
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        LOG.infof("Reconciling MirrorMaker2 %s/%s", namespace, name);

        MirrorMaker2Status status = cr.getStatus() != null ? cr.getStatus() : new MirrorMaker2Status();
        status.setPhase(MirrorMaker2Status.Phase.RECONCILING);
        status.setObservedGeneration(cr.getMetadata().getGeneration());

        // MCS placement gate
        McsConfig mcs = cr.getSpec().getMcs();
        boolean mcsEnabled = mcs != null && mcs.isEnabled();
        List<String> targetClusters = cr.getSpec().getTargetClusters();
        if (mcsEnabled && targetClusters != null && !targetClusters.isEmpty()
                && !targetClusters.contains(localClusterId)) {
            LOG.infof("MM2 %s/%s: cluster '%s' not in targetClusters %s — skipping",
                    namespace, name, localClusterId, targetClusters);
            status.setPhase(MirrorMaker2Status.Phase.SKIPPED);
            status.setMessage("Cluster '" + localClusterId + "' is not a target for this MM2");
            cr.setStatus(status);
            return UpdateControl.patchStatus(cr);
        }

        String validationError = validate(cr);
        if (validationError != null) {
            status.setPhase(MirrorMaker2Status.Phase.FAILED);
            status.setMessage(validationError);
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
            ResolvedEndpoint source = endpointResolver.resolve(cr.getSpec().getSource(), namespace);
            ResolvedEndpoint target = endpointResolver.resolve(cr.getSpec().getTarget(), namespace);
            status.setSourceBootstrap(source.bootstrap());
            status.setTargetBootstrap(target.bootstrap());

            int replicas = effectiveReplicas(cr, target);

            // Internal topics on the target managed cluster (if any).
            if (cr.getSpec().getTarget().hasManaged()) {
                String targetClusterName = cr.getSpec().getTarget().getKafkaClusterRef().getName();
                List<KafkaTopic> topics = internalTopicsBuilder.build(cr, targetClusterName, ownerRef);
                for (KafkaTopic t : topics) {
                    client.resources(KafkaTopic.class).inNamespace(namespace)
                            .resource(t).serverSideApply();
                }
            }

            // Properties + ConfigMap (configHash folds in Secret revisions so cert rotations roll).
            String properties = configBuilder.build(cr, source, target);
            String secretRevisions = secretRevisionTracker.revisionsOf(
                    secretRefs(source, target), namespace);

            // Metrics gated on spec.metricsConfig. MM2 is a Connect worker exposing metrics
            // only via JMX, so the operator bundles a fixed JMX exporter config (configMapRef
            // is not consulted — see MirrorMaker2Spec.metricsConfig).
            boolean metricsEnabled = cr.getSpec().getMetricsConfig() != null;
            String jmxConfigYaml = metricsEnabled
                    ? MetricsResources.jmxConfig("connect-jmx-config.yaml") : null;

            String configHash = ConfigHasher.sha256(properties + "::" + secretRevisions
                    + "::" + (jmxConfigYaml == null ? "" : jmxConfigYaml));
            ConfigMap cm = configMapBuilder.build(cr, properties, jmxConfigYaml, ownerRef);
            client.configMaps().inNamespace(namespace).resource(cm).serverSideApply();

            Deployment dep = deploymentBuilder.build(cr, source, target, replicas, configHash,
                    metricsEnabled, ownerRef);

            // Cross-cluster roll gate — only matters when MCS-enabled with clusterRollOrder.
            Deployment existing = client.apps().deployments().inNamespace(namespace).withName(name).get();
            if (existing != null && cr.getSpec().getClusterRollOrder() != null
                    && !cr.getSpec().getClusterRollOrder().isEmpty()) {
                boolean rollIsRequired = rollWillHappen(existing, cr.getSpec().getImage(), configHash);
                var mm2Spec = new se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec();
                mm2Spec.setClusterRollOrder(cr.getSpec().getClusterRollOrder());
                mm2Spec.setClusters(new ArrayList<>());
                if (rollIsRequired && !rollCoordinator.isMyTurnToRoll(mm2Spec, localClusterId)) {
                    LOG.infof("MM2 %s/%s: waiting for preceding cluster per clusterRollOrder %s",
                            namespace, name, cr.getSpec().getClusterRollOrder());
                    status.setMessage("Waiting for preceding cluster per spec.clusterRollOrder");
                    cr.setStatus(status);
                    return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
                }
            }

            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            if (metricsEnabled) {
                applyMm2Metrics(name, namespace, ownerRef);
            } else {
                deleteMm2Metrics(name, namespace);
            }

            if (mcsEnabled) {
                serviceExportManager.apply(name, namespace);
            }

            int ready = readyReplicas(name, namespace);
            status.setReadyReplicas(ready);
            if (ready >= replicas) {
                status.setPhase(MirrorMaker2Status.Phase.READY);
                status.setMessage(null);
            } else {
                status.setPhase(MirrorMaker2Status.Phase.PENDING);
                status.setMessage("Waiting for MM2 workers: " + ready + "/" + replicas);
                cr.setStatus(status);
                return UpdateControl.patchStatus(cr).rescheduleAfter(Duration.ofSeconds(15));
            }
        } catch (Exception e) {
            LOG.errorf("MirrorMaker2 %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(MirrorMaker2Status.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        cr.setStatus(status);
        return UpdateControl.patchStatus(cr);
    }

    @Override
    public DeleteControl cleanup(MirrorMaker2 cr, Context<MirrorMaker2> ctx) {
        // Child resources cascade via ownerReferences. Service export, however, is created
        // via apply() and needs an explicit delete in MCS mode.
        String name = cr.getMetadata().getName();
        String namespace = cr.getMetadata().getNamespace();
        McsConfig mcs = cr.getSpec().getMcs();
        if (mcs != null && mcs.isEnabled()) {
            serviceExportManager.delete(name, namespace);
        }
        deleteMm2Metrics(name, namespace);
        LOG.infof("MirrorMaker2 %s/%s deleted", namespace, name);
        return DeleteControl.defaultDelete();
    }

    /** Applies the MM2 {@code <name>-metrics} ClusterIP Service + ServiceMonitor. MM2 worker
     *  pods carry {@link Mm2Labels#labels}, so that map serves as the Service labels, pod
     *  selector, and ServiceMonitor matchLabels. */
    private void applyMm2Metrics(String name, String namespace, OwnerReference ownerRef) {
        Map<String, String> labels = Mm2Labels.labels(name);
        var svc = metricsResources.metricsService(name, namespace, labels, labels,
                "metrics", Mm2DeploymentBuilder.METRICS_PORT, List.of(ownerRef));
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        GenericKubernetesResource sm = metricsResources.serviceMonitor(name, namespace,
                labels, labels, "metrics", List.of(ownerRef));
        optionalApplier.applyServiceMonitor(sm, namespace);
    }

    private void deleteMm2Metrics(String name, String namespace) {
        client.services().inNamespace(namespace)
              .withName(name + MetricsResources.METRICS_SUFFIX).delete();
        optionalApplier.deleteServiceMonitor(name + MetricsResources.METRICS_SUFFIX, namespace);
    }

    /** Default replicas: 3 when the target is a multi-cluster managed KafkaCluster,
     *  otherwise 1. User-supplied value wins. */
    int effectiveReplicas(MirrorMaker2 cr, ResolvedEndpoint target) {
        if (cr.getSpec().getReplicas() != null && cr.getSpec().getReplicas() > 0) {
            return cr.getSpec().getReplicas();
        }
        if (cr.getSpec().getTarget().hasManaged()) {
            // Look up the target cluster to count its MCS members.
            KafkaCluster tgt = client.resources(KafkaCluster.class)
                    .inNamespace(cr.getMetadata().getNamespace())
                    .withName(cr.getSpec().getTarget().getKafkaClusterRef().getName()).get();
            if (tgt != null && tgt.getSpec().getClusters() != null
                    && tgt.getSpec().getClusters().size() > 1) {
                return 3;
            }
        }
        return 1;
    }

    private String validate(MirrorMaker2 cr) {
        if (cr.getSpec().getSource() == null) return "spec.source is required";
        if (cr.getSpec().getTarget() == null) return "spec.target is required";
        if (!cr.getSpec().getSource().hasManaged() && !cr.getSpec().getTarget().hasManaged()) {
            return "at least one of spec.source or spec.target must reference a managed KafkaCluster";
        }
        String confluentReject = rejectConfluent(cr.getSpec().getSource());
        if (confluentReject != null) return "spec.source: " + confluentReject;
        confluentReject = rejectConfluent(cr.getSpec().getTarget());
        if (confluentReject != null) return "spec.target: " + confluentReject;
        return null;
    }

    private String rejectConfluent(Mm2Endpoint ep) {
        if (ep == null || !ep.hasExternal()) return null;
        var sr = ep.getExternal().getSchemaRegistry();
        if (sr != null && sr.getType() == SchemaRegistryType.CONFLUENT) {
            return "schemaRegistry.type=CONFLUENT is not supported in v1";
        }
        return null;
    }

    private List<String> secretRefs(ResolvedEndpoint source, ResolvedEndpoint target) {
        List<String> refs = new ArrayList<>();
        if (source.tlsSecretRef() != null) refs.add(source.tlsSecretRef());
        if (target.tlsSecretRef() != null) refs.add(target.tlsSecretRef());
        if (source.hasSasl()) refs.add(source.sasl().secretRef());
        if (target.hasSasl()) refs.add(target.sasl().secretRef());
        if (source.schemaRegistryAuthSecretRef() != null) refs.add(source.schemaRegistryAuthSecretRef());
        if (target.schemaRegistryAuthSecretRef() != null) refs.add(target.schemaRegistryAuthSecretRef());
        return refs;
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
                ? meta.getAnnotations().getOrDefault(Mm2DeploymentBuilder.CONFIG_HASH_ANNOTATION, "")
                : "";
        return !desiredHash.equals(existingHash);
    }
}
