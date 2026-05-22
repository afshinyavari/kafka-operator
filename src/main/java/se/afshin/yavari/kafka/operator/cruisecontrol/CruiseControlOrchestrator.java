package se.afshin.yavari.kafka.operator.cruisecontrol;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.CruiseControlStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterCruiseControlSpec;
import se.afshin.yavari.kafka.operator.infra.ConfigHasher;
import se.afshin.yavari.kafka.operator.infra.MetricsResources;
import se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier;
import se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker;
import se.afshin.yavari.kafka.operator.rolling.RollTracker;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reconcile loop for the optional Cruise Control sub-component. Called by
 * {@link se.afshin.yavari.kafka.operator.cluster.KafkaClusterReconciler} when
 * {@code spec.cruiseControl != null}.
 *
 * <p>Unlike Apicurio (which runs per-cluster under MCS), Cruise Control is a <b>singleton</b>:
 * it is deployed only on the primary cluster ({@code spec.clusters[0]}) and reaches brokers
 * in every MCS cluster via their advertised INTERNAL listeners. Every other cluster reports
 * {@code SKIPPED}.
 */
@ApplicationScoped
public class CruiseControlOrchestrator {

    /** Fixed name for the Cruise Control Deployment + Service. */
    public static final String CC_NAME = "cruise-control";
    /** Cruise Control REST API port. */
    public static final int REST_PORT = 9090;
    /** Name of the ConfigMap holding cruisecontrol.properties + capacity.json. */
    public static final String CONFIG_MAP_NAME = "cruise-control-config";
    /** RollTracker kind key. */
    private static final String ROLL_KIND = "cruise-control";

    private static final Logger LOG = Logger.getLogger(CruiseControlOrchestrator.class);

    @Inject KubernetesClient client;
    @Inject CruiseControlConfigBuilder configBuilder;
    @Inject CruiseControlCapacityBuilder capacityBuilder;
    @Inject CruiseControlConfigMapBuilder configMapBuilder;
    @Inject CruiseControlDeploymentBuilder deploymentBuilder;
    @Inject CruiseControlServiceBuilder serviceBuilder;
    @Inject BrokerBootstrapResolver bootstrapResolver;
    @Inject SecretRevisionTracker secretRevisionTracker;
    @Inject RollTracker rollTracker;
    @Inject OptionalResourceApplier optionalApplier;
    @Inject MetricsResources metricsResources;

    /** Reconciles the Cruise Control described by {@code cr.spec.cruiseControl}. */
    public CruiseControlStatus reconcile(KafkaCluster cr, String namespace, String localClusterId) {
        String clusterName = cr.getMetadata().getName();
        KafkaClusterCruiseControlSpec spec = cr.getSpec().getCruiseControl();
        LOG.infof("Reconciling Cruise Control for KafkaCluster %s/%s", namespace, clusterName);

        CruiseControlStatus status = cr.getStatus() != null && cr.getStatus().getCruiseControl() != null
                ? cr.getStatus().getCruiseControl() : new CruiseControlStatus();
        status.setPhase(CruiseControlStatus.Phase.RECONCILING);

        // Singleton: deploy only on the primary cluster (first entry in spec.clusters).
        String primaryId = (cr.getSpec().getClusters() != null && !cr.getSpec().getClusters().isEmpty())
                ? cr.getSpec().getClusters().get(0).getId() : null;
        if (primaryId != null && !primaryId.equals(localClusterId)) {
            status.setPhase(CruiseControlStatus.Phase.SKIPPED);
            status.setMessage("Cruise Control runs only on the primary cluster '" + primaryId + "'");
            return status;
        }

        try {
            String bootstrap;
            try {
                bootstrap = bootstrapResolver.resolve(clusterName, namespace);
            } catch (BrokerBootstrapResolver.BrokerPoolNotFoundException e) {
                status.setMessage("Waiting for a broker node pool: " + e.getMessage());
                return status;
            }

            boolean mtls = cr.getSpec().getProxyMtls() != null;
            String ccCertSecret = null;
            if (mtls) {
                ccCertSecret = resolveCcCertSecret(cr);
                if (ccCertSecret == null) {
                    status.setPhase(CruiseControlStatus.Phase.FAILED);
                    status.setMessage("proxyMtls is enabled but no Cruise Control client cert "
                            + "could be resolved");
                    return status;
                }
                if (client.secrets().inNamespace(namespace).withName(ccCertSecret).get() == null) {
                    status.setMessage("Waiting for Cruise Control client cert Secret '"
                            + ccCertSecret + "'");
                    return status;
                }
            }

            String ccProps = configBuilder.build(spec, bootstrap, mtls);
            String capacityJson = capacityBuilder.build(spec.getCapacity());

            // Metrics are gated on the parent cluster's spec.metricsConfig. Cruise Control
            // exposes metrics only via JMX, so the operator bundles a fixed JMX exporter
            // config (configMapRef is not consulted here — it is broker-only).
            boolean metricsEnabled = cr.getSpec().getMetricsConfig() != null;
            String jmxConfigYaml = metricsEnabled
                    ? MetricsResources.jmxConfig("cruise-control-jmx-config.yaml") : null;

            ConfigMap cm = configMapBuilder.build(namespace, ccProps, capacityJson, jmxConfigYaml);
            cm.getMetadata().setOwnerReferences(clusterOwnerRef(cr));
            client.configMaps().inNamespace(namespace).resource(cm).serverSideApply();

            String secretRevisions = mtls
                    ? secretRevisionTracker.revisionsOf(List.of(ccCertSecret), namespace) : "";
            String configHash = ConfigHasher.sha256(ccProps, capacityJson, secretRevisions,
                    jmxConfigYaml == null ? "" : jmxConfigYaml);

            Deployment dep = deploymentBuilder.build(spec, namespace, configHash, mtls,
                    ccCertSecret, cr.getSpec().getKafkaImage(), metricsEnabled);
            dep.getMetadata().setOwnerReferences(clusterOwnerRef(cr));

            Deployment existing = client.apps().deployments()
                    .inNamespace(namespace).withName(CC_NAME).get();
            if (rollWillHappen(existing, spec.getImage(), configHash)) {
                rollTracker.markRolling(ROLL_KIND, namespace, CC_NAME);
            }
            client.apps().deployments().inNamespace(namespace).resource(dep).serverSideApply();

            Service svc = serviceBuilder.build(namespace);
            svc.getMetadata().setOwnerReferences(clusterOwnerRef(cr));
            client.services().inNamespace(namespace).resource(svc).serverSideApply();

            if (metricsEnabled) {
                applyCcMetrics(namespace, clusterOwnerRef(cr));
            } else {
                deleteCcMetrics(namespace);
            }

            int ready = readyReplicas(namespace);
            if (ready >= 1) {
                status.setPhase(CruiseControlStatus.Phase.READY);
                status.setMessage(null);
                status.setUrl("http://" + CC_NAME + "." + namespace
                        + ".svc.cluster.local:" + REST_PORT);
                rollTracker.markComplete(ROLL_KIND, namespace, CC_NAME);
            } else {
                status.setMessage("Waiting for the Cruise Control pod");
            }
        } catch (Exception e) {
            LOG.errorf("Cruise Control for %s/%s failed: %s", namespace, clusterName, e.getMessage());
            status.setPhase(CruiseControlStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }
        return status;
    }

    public void cleanup(KafkaCluster cr, String namespace) {
        client.apps().deployments().inNamespace(namespace).withName(CC_NAME).delete();
        client.services().inNamespace(namespace).withName(CC_NAME).delete();
        client.configMaps().inNamespace(namespace).withName(CONFIG_MAP_NAME).delete();
        deleteCcMetrics(namespace);
    }

    /** Applies the {@code cruise-control-metrics} ClusterIP Service + ServiceMonitor. The
     *  Cruise Control pod carries {@link CruiseControlDeploymentBuilder#labels}, so that map
     *  serves as Service labels, pod selector, and ServiceMonitor matchLabels. */
    private void applyCcMetrics(String namespace, List<OwnerReference> owner) {
        Map<String, String> labels = CruiseControlDeploymentBuilder.labels();
        Service svc = metricsResources.metricsService(CC_NAME, namespace, labels, labels,
                "metrics", CruiseControlDeploymentBuilder.METRICS_PORT, owner);
        client.services().inNamespace(namespace).resource(svc).serverSideApply();
        GenericKubernetesResource sm = metricsResources.serviceMonitor(CC_NAME, namespace,
                labels, labels, "metrics", owner);
        optionalApplier.applyServiceMonitor(sm, namespace);
    }

    private void deleteCcMetrics(String namespace) {
        client.services().inNamespace(namespace)
              .withName(CC_NAME + MetricsResources.METRICS_SUFFIX).delete();
        optionalApplier.deleteServiceMonitor(CC_NAME + MetricsResources.METRICS_SUFFIX, namespace);
    }

    /** True when the cluster's Cruise Control config references the given Secret. Used by the
     *  KafkaClusterReconciler Secret informer so cert / credential rotation rolls Cruise Control. */
    public static boolean referencesSecret(KafkaCluster cr, String secretName) {
        if (cr.getSpec().getCruiseControl() == null || secretName == null) return false;
        KafkaClusterCruiseControlSpec cc = cr.getSpec().getCruiseControl();
        if (cr.getSpec().getProxyMtls() != null && secretName.equals(resolveCcCertSecret(cr))) {
            return true;
        }
        return cc.getApiSecurity() != null
                && secretName.equals(cc.getApiSecurity().getBasicAuthSecretRef());
    }

    /** Resolves the Cruise Control client cert Secret — the explicit ref, else the operator's
     *  shared AdminClient cert. Returns null when mTLS is off / unresolvable. */
    static String resolveCcCertSecret(KafkaCluster cr) {
        KafkaClusterCruiseControlSpec cc = cr.getSpec().getCruiseControl();
        if (cc != null && cc.getBrokerClientCertSecretRef() != null
                && !cc.getBrokerClientCertSecretRef().isBlank()) {
            return cc.getBrokerClientCertSecretRef();
        }
        return cr.getSpec().getProxyMtls() != null
                ? cr.getSpec().getProxyMtls().resolveAdminClientCertSecret() : null;
    }

    static boolean rollWillHappen(Deployment existing, String desiredImage, String desiredHash) {
        if (existing == null || existing.getSpec() == null
                || existing.getSpec().getTemplate() == null) {
            return true;
        }
        var podSpec = existing.getSpec().getTemplate().getSpec();
        String existingImage = (podSpec != null && podSpec.getContainers() != null
                && !podSpec.getContainers().isEmpty())
                ? podSpec.getContainers().get(0).getImage() : null;
        if (desiredImage != null && !Objects.equals(desiredImage, existingImage)) return true;
        var meta = existing.getSpec().getTemplate().getMetadata();
        String existingHash = (meta != null && meta.getAnnotations() != null)
                ? meta.getAnnotations().getOrDefault(
                        CruiseControlDeploymentBuilder.CONFIG_HASH_ANNOTATION, "")
                : "";
        return !desiredHash.equals(existingHash);
    }

    private int readyReplicas(String namespace) {
        Deployment dep = client.apps().deployments().inNamespace(namespace).withName(CC_NAME).get();
        if (dep == null || dep.getStatus() == null || dep.getStatus().getReadyReplicas() == null) {
            return 0;
        }
        return dep.getStatus().getReadyReplicas();
    }

    private static List<OwnerReference> clusterOwnerRef(KafkaCluster cr) {
        return List.of(new OwnerReferenceBuilder()
                .withApiVersion(cr.getApiVersion())
                .withKind(cr.getKind())
                .withName(cr.getMetadata().getName())
                .withUid(cr.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build());
    }
}
