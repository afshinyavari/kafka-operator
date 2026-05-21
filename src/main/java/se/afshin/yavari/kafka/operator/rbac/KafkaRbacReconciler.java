package se.afshin.yavari.kafka.operator.rbac;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaRbac;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaRbacUser;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;
import se.afshin.yavari.kafka.operator.topic.KafkaTopicService;
import se.afshin.yavari.kafka.operator.topic.TopicReconcileLeader;

import java.util.List;
import java.util.stream.Collectors;

@ControllerConfiguration
@ApplicationScoped
public class KafkaRbacReconciler implements Reconciler<KafkaRbac>, Cleaner<KafkaRbac> {

    private static final Logger LOG = Logger.getLogger(KafkaRbacReconciler.class);

    @Inject KubernetesClient client;
    @Inject KafkaRbacConfigMapBuilder configMapBuilder;
    @Inject KafkaQuotaManager quotaManager;
    @Inject BrokerBootstrapResolver bootstrapResolver;
    @Inject AdminClientTlsLoader tlsLoader;
    @Inject KafkaTopicService topicService;
    @Inject TopicReconcileLeader leader;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public UpdateControl<KafkaRbac> reconcile(KafkaRbac rbac, Context<KafkaRbac> context) {
        try (var ignored = se.afshin.yavari.kafka.operator.infra.ReconcileContext.scope(rbac)) {
        return reconcileInner(rbac, context);
        }
    }

    private UpdateControl<KafkaRbac> reconcileInner(KafkaRbac rbac, Context<KafkaRbac> context) {
        String name = rbac.getMetadata().getName();
        String namespace = rbac.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaRbac %s/%s", namespace, name);

        KafkaRbacStatus status = rbac.getStatus() != null ? rbac.getStatus() : new KafkaRbacStatus();
        status.setPhase(KafkaRbacStatus.Phase.RECONCILING);

        try {
            ConfigMap kafkaCm = configMapBuilder.buildKafkaRules(rbac, namespace);
            client.configMaps().inNamespace(namespace).resource(kafkaCm).serverSideApply();

            ConfigMap apicurioCm = configMapBuilder.buildApicurioPolicy(rbac, namespace);
            client.configMaps().inNamespace(namespace).resource(apicurioCm).serverSideApply();

            // Apply per-user Kafka client quotas, if any. Best-effort: failure is logged
            // but does not flip the RBAC reconcile to FAILED — the ConfigMaps above are
            // the load-bearing artifacts; quotas are an optional broker-side overlay.
            List<KafkaRbacUser> usersWithQuotas = rbac.getSpec().getUsers().stream()
                    .filter(u -> u.getQuotas() != null).collect(Collectors.toList());
            if (!usersWithQuotas.isEmpty()) {
                applyQuotas(rbac, namespace, usersWithQuotas);
            }

            status.setPhase(KafkaRbacStatus.Phase.READY);
            status.setMessage(null);
        } catch (Exception e) {
            LOG.errorf("KafkaRbac %s/%s failed: %s", namespace, name, e.getMessage());
            status.setPhase(KafkaRbacStatus.Phase.FAILED);
            status.setMessage(e.getMessage());
        }

        rbac.setStatus(status);
        return UpdateControl.patchStatus(rbac);
    }

    private void applyQuotas(KafkaRbac rbac, String namespace, List<KafkaRbacUser> users) {
        // KafkaRbac doesn't carry a clusterRef — find the namespace's KafkaCluster.
        // Single-cluster-per-namespace is the assumed model post-Wave-4.
        List<KafkaCluster> clusters = client.resources(KafkaCluster.class)
                .inNamespace(namespace).list().getItems();
        if (clusters.size() != 1) {
            LOG.infof("KafkaRbac %s/%s: skipping quota apply — %d KafkaClusters in namespace "
                    + "(expected exactly 1)", namespace, rbac.getMetadata().getName(), clusters.size());
            return;
        }
        KafkaCluster cluster = clusters.get(0);
        // Apply quotas only from the primary cluster — quotas are cluster-wide (stored in
        // __cluster_metadata) so every cluster applying them would just produce duplicate
        // writes. (TopicReconcileLeader's isLeader() takes a KafkaTopic; the cluster check
        // is the same either way, so go through currentLeaderId() directly.)
        String leaderId = leader.currentLeaderId(cluster);
        if (leaderId == null || !leaderId.equals(localClusterId)) {
            LOG.debugf("KafkaRbac %s/%s: not primary cluster — skipping quota apply",
                    namespace, rbac.getMetadata().getName());
            return;
        }
        String bootstrap;
        try {
            bootstrap = bootstrapResolver.resolve(cluster.getMetadata().getName(), namespace);
        } catch (BrokerBootstrapResolver.BrokerPoolNotFoundException e) {
            LOG.warnf("KafkaRbac %s/%s: cannot resolve broker bootstrap for quotas: %s",
                    namespace, rbac.getMetadata().getName(), e.getMessage());
            return;
        }
        java.util.Properties sslProps = null;
        KafkaProxyMtlsConfig mtls = cluster.getSpec().getProxyMtls();
        if (mtls != null) {
            try {
                sslProps = tlsLoader.loadAsAdminClientSslProps(namespace,
                        mtls.resolveAdminClientCertSecret());
            } catch (AdminClientTlsLoader.TlsSecretNotFoundException e) {
                LOG.warnf("KafkaRbac %s/%s: AdminClient TLS secret missing: %s",
                        namespace, rbac.getMetadata().getName(), e.getMessage());
                return;
            }
        }
        try (AdminClient admin = topicService.newAdmin(bootstrap, sslProps)) {
            List<String> errors = quotaManager.applyUserQuotas(admin, users);
            if (!errors.isEmpty()) {
                LOG.warnf("KafkaRbac %s/%s: quota apply had errors: %s",
                        namespace, rbac.getMetadata().getName(), errors);
            }
        }
    }

    @Override
    public DeleteControl cleanup(KafkaRbac rbac, Context<KafkaRbac> context) {
        LOG.infof("KafkaRbac %s deleted — ConfigMaps cascade via owner reference",
                rbac.getMetadata().getName());
        return DeleteControl.defaultDelete();
    }
}
