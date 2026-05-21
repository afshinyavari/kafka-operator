package se.afshin.yavari.kafka.operator.topic;

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
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicStatus;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;

import java.time.Duration;
import java.time.Instant;

@ControllerConfiguration
@ApplicationScoped
public class KafkaTopicReconciler implements Reconciler<KafkaTopic>, Cleaner<KafkaTopic> {

    private static final Logger LOG = Logger.getLogger(KafkaTopicReconciler.class);

    static final Duration RECHECK_INTERVAL = Duration.ofMinutes(5);
    static final Duration CLEANUP_RETRY_INTERVAL = Duration.ofSeconds(15);

    @Inject KubernetesClient client;
    @Inject KafkaTopicService service;
    @Inject TopicReconcileLeader leader;
    @Inject BrokerBootstrapResolver bootstrapResolver;
    @Inject AdminClientTlsLoader tlsLoader;

    @ConfigProperty(name = "kafka.cluster.id")
    String localClusterId;

    @Override
    public UpdateControl<KafkaTopic> reconcile(KafkaTopic topic, Context<KafkaTopic> ctx) {
        try (var ignored = se.afshin.yavari.kafka.operator.infra.ReconcileContext.scope(topic)) {
        return reconcileInner(topic, ctx);
        }
    }

    private UpdateControl<KafkaTopic> reconcileInner(KafkaTopic topic, Context<KafkaTopic> ctx) {
        String ns = topic.getMetadata().getNamespace();
        String name = topic.getMetadata().getName();
        String topicName = topic.resolvedTopicName();
        LOG.infof("Reconciling KafkaTopic %s/%s (topic=%s)", ns, name, topicName);

        KafkaTopicStatus status = topic.getStatus() != null ? topic.getStatus() : new KafkaTopicStatus();
        status.setObservedGeneration(topic.getMetadata().getGeneration());
        status.setLastReconcileTime(Instant.now().toString());

        KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                .withName(topic.getSpec().getClusterRef()).get();
        if (cluster == null) {
            return fail(topic, status, "Referenced KafkaCluster '" + topic.getSpec().getClusterRef()
                    + "' not found in namespace " + ns);
        }

        if (!leader.isLeader(topic, cluster, localClusterId)) {
            status.setPhase(KafkaTopicStatus.Phase.SKIPPED);
            status.setMessage("Reconciled by primary cluster '" + leader.currentLeaderId(cluster) + "'");
            topic.setStatus(status);
            return UpdateControl.patchStatus(topic);
        }

        String bootstrap;
        try {
            bootstrap = bootstrapResolver.resolve(topic.getSpec().getClusterRef(), ns);
        } catch (BrokerBootstrapResolver.BrokerPoolNotFoundException e) {
            return fail(topic, status, e.getMessage());
        }

        java.util.Properties sslProps = null;
        var mtls = cluster.getSpec().getProxyMtls();
        if (mtls != null) {
            try {
                sslProps = tlsLoader.loadAsAdminClientSslProps(ns, mtls.resolveAdminClientCertSecret());
            } catch (AdminClientTlsLoader.TlsSecretNotFoundException e) {
                return fail(topic, status, e.getMessage());
            }
        }

        try (AdminClient admin = service.newAdmin(bootstrap, sslProps)) {
            var state = service.describe(admin, topicName);
            if (state == null) {
                LOG.infof("Topic %s missing — creating with partitions=%d rf=%d",
                        topicName, topic.getSpec().getPartitions(), topic.getSpec().getReplicationFactor());
                state = service.createTopic(admin, topicName,
                        topic.getSpec().getPartitions(),
                        (short) topic.getSpec().getReplicationFactor(),
                        topic.getSpec().getConfig());
            } else {
                if (state.replicationFactor() != topic.getSpec().getReplicationFactor()) {
                    return fail(topic, status,
                            "ReplicationFactor mismatch (topic=" + state.replicationFactor()
                                    + ", spec=" + topic.getSpec().getReplicationFactor()
                                    + ") — RF changes are not supported; reassign partitions manually or recreate");
                }
                var partitionAction = service.computePartitionAction(
                        state.partitions(), topic.getSpec().getPartitions());
                switch (partitionAction) {
                    case EXPAND -> {
                        LOG.infof("Expanding %s from %d to %d partitions",
                                topicName, state.partitions(), topic.getSpec().getPartitions());
                        service.increasePartitions(admin, topicName, topic.getSpec().getPartitions());
                    }
                    case REJECT_DECREASE -> {
                        return fail(topic, status,
                                "Partition decrease not supported (topic=" + state.partitions()
                                        + ", spec=" + topic.getSpec().getPartitions() + ")");
                    }
                    case NONE -> { /* no-op */ }
                }
                var ops = service.computeConfigDiff(state.dynamicConfig(), topic.getSpec().getConfig());
                if (!ops.isEmpty()) {
                    LOG.infof("Applying %d config change(s) to %s", ops.size(), topicName);
                    service.applyConfigDiff(admin, topicName, ops);
                }
                state = service.describe(admin, topicName);
            }

            status.setPhase(KafkaTopicStatus.Phase.READY);
            status.setMessage(null);
            if (state != null) {
                status.setTopicId(state.topicId());
                status.setObservedPartitions(state.partitions());
                status.setObservedReplicationFactor(state.replicationFactor());
            }
            topic.setStatus(status);
            return UpdateControl.patchStatus(topic).rescheduleAfter(RECHECK_INTERVAL);

        } catch (Exception e) {
            LOG.errorf(e, "KafkaTopic %s/%s reconcile failed", ns, name);
            return fail(topic, status, e.getMessage() != null ? e.getMessage() : e.toString());
        }
    }

    @Override
    public DeleteControl cleanup(KafkaTopic topic, Context<KafkaTopic> ctx) {
        String ns = topic.getMetadata().getNamespace();
        String topicName = topic.resolvedTopicName();

        KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                .withName(topic.getSpec().getClusterRef()).get();
        if (cluster == null) {
            LOG.infof("KafkaTopic %s/%s — cluster gone, releasing finalizer", ns, topic.getMetadata().getName());
            return DeleteControl.defaultDelete();
        }
        if (!leader.isLeader(topic, cluster, localClusterId)) {
            return DeleteControl.defaultDelete();
        }
        if (topic.getSpec().getDeletionPolicy() == TopicDeletionPolicy.RETAIN) {
            LOG.infof("KafkaTopic %s/%s deletion policy=RETAIN — leaving topic '%s' in place",
                    ns, topic.getMetadata().getName(), topicName);
            return DeleteControl.defaultDelete();
        }

        String bootstrap;
        try {
            bootstrap = bootstrapResolver.resolve(topic.getSpec().getClusterRef(), ns);
        } catch (BrokerBootstrapResolver.BrokerPoolNotFoundException e) {
            LOG.warnf("KafkaTopic %s/%s — no broker pool to delete topic via; releasing finalizer", ns, topicName);
            return DeleteControl.defaultDelete();
        }

        java.util.Properties sslProps = null;
        var mtls = cluster.getSpec().getProxyMtls();
        if (mtls != null) {
            try {
                sslProps = tlsLoader.loadAsAdminClientSslProps(ns, mtls.resolveAdminClientCertSecret());
            } catch (AdminClientTlsLoader.TlsSecretNotFoundException e) {
                LOG.warnf("KafkaTopic %s/%s cleanup — TLS secret missing (%s) — releasing finalizer to unblock CR deletion",
                        ns, topicName, e.getMessage());
                return DeleteControl.defaultDelete();
            }
        }

        try (AdminClient admin = service.newAdmin(bootstrap, sslProps)) {
            service.deleteTopic(admin, topicName);
            LOG.infof("Deleted topic %s in Kafka", topicName);
            return DeleteControl.defaultDelete();
        } catch (Exception e) {
            LOG.warnf("Topic delete for %s failed (%s) — will retry, keeping finalizer",
                    topicName, e.getMessage());
            return DeleteControl.noFinalizerRemoval().rescheduleAfter(CLEANUP_RETRY_INTERVAL);
        }
    }

    private UpdateControl<KafkaTopic> fail(KafkaTopic topic, KafkaTopicStatus status, String msg) {
        status.setPhase(KafkaTopicStatus.Phase.FAILED);
        status.setMessage(msg);
        topic.setStatus(status);
        return UpdateControl.patchStatus(topic).rescheduleAfter(RECHECK_INTERVAL);
    }
}
