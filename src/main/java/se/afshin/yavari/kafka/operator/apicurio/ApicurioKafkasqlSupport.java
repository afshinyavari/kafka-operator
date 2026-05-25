package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.acl.KafkaAclManager;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicSpec;
import se.afshin.yavari.kafka.operator.infra.OwnerReferences;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicStatus;
import se.afshin.yavari.kafka.operator.crd.TopicDeletionPolicy;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates the kafkasql-storage preflight that {@link ApicurioRegistryReconciler}
 * runs before building the registry Deployment: validates the spec, ensures the journal
 * {@link KafkaTopic} CR, and provisions broker ACLs for the registry's mTLS principal.
 *
 * <p>The result is a sealed interface — the reconciler maps {@code Pending} →
 * {@code RECONCILING} (with reschedule), {@code Failed} → {@code FAILED}, and only
 * {@code Ready} continues into the Deployment build.
 */
@ApplicationScoped
public class ApicurioKafkasqlSupport {

    private static final Logger LOG = Logger.getLogger(ApicurioKafkasqlSupport.class);

    @Inject KubernetesClient client;
    @Inject BrokerBootstrapResolver bootstrapResolver;
    @Inject AdminClientTlsLoader tlsLoader;
    @Inject KafkaAclManager aclManager;

    public sealed interface Result {
        record Pending(String reason) implements Result {}
        record Ready(ApicurioDeploymentBuilder.KafkasqlConfig config) implements Result {}
        record Failed(String message) implements Result {}
    }

    public Result prepare(ApicurioRegistry registry) {
        String ns = registry.getMetadata().getNamespace();
        ApicurioRegistryStorageConfig storage = registry.getSpec().getStorage();

        if (storage.getClusterRef() == null || storage.getClusterRef().isBlank()) {
            return new Result.Failed("storage.clusterRef is required when storage.type=kafkasql");
        }

        KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                .withName(storage.getClusterRef()).get();
        if (cluster == null) {
            return new Result.Failed("KafkaCluster " + ns + "/" + storage.getClusterRef() + " not found");
        }

        KafkaProxyMtlsConfig mtls = cluster.getSpec().getProxyMtls();
        boolean mtlsEnabled = mtls != null;
        if (mtlsEnabled) {
            if (storage.getTlsSecretRef() == null || storage.getTlsSecretRef().isBlank()) {
                return new Result.Failed("storage.tlsSecretRef is required when the referenced "
                        + "KafkaCluster has proxyMtls.enabled=true");
            }
            if (storage.getPrincipal() == null || storage.getPrincipal().isBlank()) {
                return new Result.Failed("storage.principal is required when the referenced "
                        + "KafkaCluster has proxyMtls.enabled=true");
            }
            try {
                tlsLoader.validateSecretShape(ns, storage.getTlsSecretRef());
            } catch (AdminClientTlsLoader.TlsSecretNotFoundException e) {
                return new Result.Failed(e.getMessage());
            }
        }

        String bootstrap;
        try {
            bootstrap = bootstrapResolver.resolve(storage.getClusterRef(), ns);
        } catch (BrokerBootstrapResolver.BrokerPoolNotFoundException e) {
            return new Result.Failed(e.getMessage());
        }

        KafkaTopic topic = ensureJournalTopic(registry, storage);
        KafkaTopicStatus topicStatus = topic.getStatus();
        // SKIPPED means this operator is not the primary cluster for the KafkaTopic CR; the
        // primary cluster's operator owns the actual AdminClient create. We trust that and
        // proceed — the journal exists in Kafka regardless of which operator created it.
        boolean topicUsable = topicStatus != null
                && (topicStatus.getPhase() == KafkaTopicStatus.Phase.READY
                    || topicStatus.getPhase() == KafkaTopicStatus.Phase.SKIPPED);
        if (!topicUsable) {
            String phase = topicStatus == null ? "<none>" : String.valueOf(topicStatus.getPhase());
            return new Result.Pending("waiting for kafkasql journal topic '"
                    + topic.getMetadata().getName() + "' (phase=" + phase + ")");
        }

        if (mtlsEnabled) {
            try {
                aclManager.apply(storage.getClusterRef(), ns,
                        mtls.resolveAdminClientCertSecret(),
                        registryAcls(storage));
            } catch (KafkaAclManager.AclProvisioningException e) {
                return new Result.Failed("Failed to provision ACLs: " + e.getMessage());
            }
        }

        return new Result.Ready(new ApicurioDeploymentBuilder.KafkasqlConfig(
                bootstrap, storage.getKafkaTopic(),
                mtlsEnabled ? storage.getTlsSecretRef() : null,
                cluster.getSpec().getKafkaImage()));
    }

    public void cleanup(ApicurioRegistry registry) {
        ApicurioRegistryStorageConfig storage = registry.getSpec().getStorage();
        if (storage == null || !"kafkasql".equals(storage.getType())) return;

        String ns = registry.getMetadata().getNamespace();
        KafkaCluster cluster = client.resources(KafkaCluster.class).inNamespace(ns)
                .withName(storage.getClusterRef()).get();
        if (cluster == null) return; // nothing to clean

        KafkaProxyMtlsConfig mtls = cluster.getSpec().getProxyMtls();
        if (mtls == null || storage.getPrincipal() == null) return;

        try {
            aclManager.delete(storage.getClusterRef(), ns,
                    mtls.resolveAdminClientCertSecret(),
                    principalOf(storage));
        } catch (KafkaAclManager.AclProvisioningException e) {
            LOG.warnf("Failed to delete ACLs for %s/%s: %s",
                    ns, registry.getMetadata().getName(), e.getMessage());
        }
    }

    private KafkaTopic ensureJournalTopic(ApicurioRegistry registry, ApicurioRegistryStorageConfig storage) {
        String ns = registry.getMetadata().getNamespace();
        String topicCrName = registry.getMetadata().getName() + "-kafkasql-journal";

        KafkaTopic desired = new KafkaTopic();
        var meta = new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName(topicCrName)
                .withNamespace(ns);
        // Post-Wave-4c: the synthetic ApicurioRegistry has no UID — owner-ref the parent
        // KafkaCluster instead (looked up via storage.clusterRef which the orchestrator
        // populates). On the legacy path the registry IS a CR and has a real UID; we
        // honour that when present.
        // ApicurioRegistry is a synthetic POJO (not HasMetadata) so the OwnerReferences util
        // cannot resolve it; the owner-ref is built inline here.
        if (registry.getMetadata().getUid() != null && !registry.getMetadata().getUid().isBlank()) {
            meta.withOwnerReferences(new OwnerReferenceBuilder()
                    .withApiVersion(registry.getApiVersion())
                    .withKind(registry.getKind())
                    .withName(registry.getMetadata().getName())
                    .withUid(registry.getMetadata().getUid())
                    .withController(true)
                    .withBlockOwnerDeletion(true)
                    .build());
        } else if (storage.getClusterRef() != null) {
            KafkaCluster owner = client.resources(KafkaCluster.class).inNamespace(ns)
                    .withName(storage.getClusterRef()).get();
            if (owner != null && owner.getMetadata().getUid() != null) {
                meta.withOwnerReferences(OwnerReferences.of(owner));
            }
        }
        desired.setMetadata(meta.build());

        KafkaTopicSpec spec = new KafkaTopicSpec();
        spec.setClusterRef(storage.getClusterRef());
        spec.setTopicName(storage.getKafkaTopic());
        // Apicurio v2.6 documents single-partition for kafkasql ordering; the
        // CRD allows overriding via storage.kafkaTopicPartitions for experimentation.
        int partitions = storage.getKafkaTopicPartitions() != null
                ? storage.getKafkaTopicPartitions() : 1;
        if (partitions > 1) {
            LOG.warnf("ApicurioRegistry %s/%s: kafkasql journal partitions=%d. Apicurio v2.6 "
                    + "requires partitions=1 for total ordering across writers; multi-cluster HA "
                    + "(#19) relies on this. Override at your own risk.",
                    ns, registry.getMetadata().getName(), partitions);
        }
        spec.setPartitions(partitions);
        spec.setReplicationFactor(3);
        spec.setConfig(Map.of(
                "cleanup.policy", "compact",
                "min.insync.replicas", "2",
                "min.compaction.lag.ms", "0",
                "segment.ms", "86400000"));
        spec.setDeletionPolicy(TopicDeletionPolicy.RETAIN);
        desired.setSpec(spec);

        return client.resources(KafkaTopic.class).inNamespace(ns)
                .resource(desired).serverSideApply();
    }

    private static List<AclBinding> registryAcls(ApicurioRegistryStorageConfig storage) {
        String principal = principalOf(storage);
        return List.of(
                topicAcl(principal, storage.getKafkaTopic(), AclOperation.READ),
                topicAcl(principal, storage.getKafkaTopic(), AclOperation.WRITE),
                topicAcl(principal, storage.getKafkaTopic(), AclOperation.DESCRIBE),
                groupAcl(principal, "apicurio-registry", AclOperation.READ),
                groupAcl(principal, "apicurio-registry", AclOperation.DESCRIBE),
                clusterAcl(principal, AclOperation.DESCRIBE));
    }

    private static String principalOf(ApicurioRegistryStorageConfig storage) {
        return "User:CN=" + storage.getPrincipal();
    }

    private static AclBinding topicAcl(String principal, String topic, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
    }

    private static AclBinding groupAcl(String principal, String prefix, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.GROUP, prefix, PatternType.PREFIXED),
                new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
    }

    private static AclBinding clusterAcl(String principal, AclOperation op) {
        return new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
                new AccessControlEntry(principal, "*", op, AclPermissionType.ALLOW));
    }
}
