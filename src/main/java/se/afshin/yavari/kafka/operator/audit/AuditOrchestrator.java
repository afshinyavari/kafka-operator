package se.afshin.yavari.kafka.operator.audit;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.AuditKafkaTopicSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterAuditSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaTopic;
import se.afshin.yavari.kafka.operator.crd.KafkaTopicSpec;
import se.afshin.yavari.kafka.operator.proxy.KafkaProxyOrchestrator;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns the cluster-level audit resources: the {@link KafkaTopic} that the audit emitter
 * ships to and the env+volume contract the proxy and Apicurio rbac-proxy Deployments use
 * to wire up the in-process emitter.
 *
 * <p>Per design, the in-process stdout sink is <b>always</b> active (zero config);
 * this orchestrator only activates the opt-in Kafka-topic sink. Audit traffic always
 * connects directly to the broker INTERNAL listener (bypassing Kroxylicious) so there
 * is no audit-the-audit loop.
 */
@ApplicationScoped
public class AuditOrchestrator {

    /** Volume name used on every audit-emitting workload. */
    public static final String AUDIT_TLS_VOLUME = "audit-tls";
    /** Where the audit producer's mTLS PEMs are mounted inside the container. */
    public static final String AUDIT_TLS_MOUNT = "/etc/audit-tls";

    private static final Logger LOG = Logger.getLogger(AuditOrchestrator.class);

    @Inject KubernetesClient client;
    @Inject BrokerBootstrapResolver bootstrapResolver;

    /** True iff {@code spec.audit.kafkaTopic.enabled} is on. */
    public static boolean kafkaSinkEnabled(KafkaCluster cr) {
        KafkaClusterAuditSpec a = cr.getSpec().getAudit();
        return a != null && a.getKafkaTopic() != null && a.getKafkaTopic().isEnabled();
    }

    /**
     * Upserts the audit {@link KafkaTopic} on the parent cluster. No-op when the
     * Kafka sink is disabled. Called from {@code KafkaClusterReconciler} after the
     * proxy + Apicurio reconciles so the topic lands before pods that would write to it.
     */
    public void reconcile(KafkaCluster cr, String namespace) {
        if (!kafkaSinkEnabled(cr)) return;
        AuditKafkaTopicSpec sink = cr.getSpec().getAudit().getKafkaTopic();
        String clusterName = cr.getMetadata().getName();
        String topicCrName = clusterName + "-audit";

        Map<String, String> topicCfg = new LinkedHashMap<>();
        topicCfg.put("cleanup.policy", "delete");
        topicCfg.put("retention.ms", String.valueOf(sink.getRetentionDays() * 86_400_000L));
        topicCfg.put("compression.type", "zstd");

        KafkaTopic topic = new KafkaTopic();
        topic.setMetadata(new ObjectMetaBuilder()
                .withName(topicCrName)
                .withNamespace(namespace)
                .withOwnerReferences(clusterOwnerRef(cr))
                .build());
        KafkaTopicSpec spec = new KafkaTopicSpec();
        spec.setClusterRef(clusterName);
        spec.setTopicName(sink.getName());
        spec.setPartitions(sink.getPartitions());
        spec.setReplicationFactor(sink.getReplicationFactor());
        spec.setConfig(topicCfg);
        topic.setSpec(spec);

        client.resources(KafkaTopic.class).inNamespace(namespace).resource(topic).serverSideApply();
        LOG.debugf("Upserted audit KafkaTopic %s/%s (target topic '%s')",
                namespace, topicCrName, sink.getName());
    }

    /**
     * Env vars to add to every audit-emitting container. Empty list when the Kafka sink
     * is disabled (the always-on stdout sink needs no env).
     */
    public List<EnvVar> envVars(KafkaCluster cr, String namespace) {
        if (!kafkaSinkEnabled(cr)) return List.of();
        AuditKafkaTopicSpec sink = cr.getSpec().getAudit().getKafkaTopic();
        // Direct connection to broker INTERNAL listener — bypasses Kroxylicious so there's no
        // loop, and avoids the proxy needing to authorise audit producers separately.
        String bootstrap = bootstrapResolver.resolve(cr.getMetadata().getName(), namespace);
        return List.of(
                env("KAFKA_AUDIT_BOOTSTRAP", bootstrap),
                env("KAFKA_AUDIT_TOPIC", sink.getName()),
                env("KAFKA_AUDIT_TLS_CERT", AUDIT_TLS_MOUNT + "/tls.crt"),
                env("KAFKA_AUDIT_TLS_KEY",  AUDIT_TLS_MOUNT + "/tls.key"),
                env("KAFKA_AUDIT_TLS_CA",   AUDIT_TLS_MOUNT + "/ca.crt"));
    }

    /** {@code audit-tls} Volume backed by the proxy's existing client cert Secret. */
    public Volume tlsVolume(KafkaCluster cr) {
        String secret = resolveClientCertSecret(cr);
        return new VolumeBuilder()
                .withName(AUDIT_TLS_VOLUME)
                .withNewSecret().withSecretName(secret).endSecret()
                .build();
    }

    /** Mount of {@link #tlsVolume(KafkaCluster)} at {@link #AUDIT_TLS_MOUNT}. */
    public VolumeMount tlsMount() {
        return new VolumeMountBuilder()
                .withName(AUDIT_TLS_VOLUME)
                .withMountPath(AUDIT_TLS_MOUNT)
                .withReadOnly(true)
                .build();
    }

    /**
     * Mutates a built proxy Deployment to add the audit env vars + tls volume/mount.
     * Idempotent — calling twice does not duplicate entries. No-op when the Kafka sink
     * is disabled.
     */
    public void injectIntoDeployment(Deployment dep, KafkaCluster cr, String namespace) {
        if (!kafkaSinkEnabled(cr) || dep == null || dep.getSpec() == null
                || dep.getSpec().getTemplate() == null
                || dep.getSpec().getTemplate().getSpec() == null) return;
        var podSpec = dep.getSpec().getTemplate().getSpec();
        addVolumeIfMissing(podSpec.getVolumes(), tlsVolume(cr), v -> podSpec.setVolumes(v));
        List<EnvVar> envs = envVars(cr, namespace);
        VolumeMount mount = tlsMount();
        if (podSpec.getContainers() != null) {
            for (Container c : podSpec.getContainers()) {
                injectIntoContainer(c, envs, mount);
            }
        }
    }

    /** Mutates a built container before it goes into a Deployment. Idempotent. */
    public void injectIntoContainer(Container c, KafkaCluster cr, String namespace) {
        if (!kafkaSinkEnabled(cr) || c == null) return;
        injectIntoContainer(c, envVars(cr, namespace), tlsMount());
    }

    private static void injectIntoContainer(Container c, List<EnvVar> envs, VolumeMount mount) {
        List<EnvVar> existingEnv = c.getEnv();
        if (existingEnv == null) {
            c.setEnv(new ArrayList<>(envs));
        } else {
            for (EnvVar e : envs) {
                boolean already = existingEnv.stream().anyMatch(x -> e.getName().equals(x.getName()));
                if (!already) existingEnv.add(e);
            }
        }
        List<VolumeMount> existingMounts = c.getVolumeMounts();
        if (existingMounts == null) {
            c.setVolumeMounts(new ArrayList<>(List.of(mount)));
        } else if (existingMounts.stream().noneMatch(m -> AUDIT_TLS_VOLUME.equals(m.getName()))) {
            existingMounts.add(mount);
        }
    }

    private static void addVolumeIfMissing(List<Volume> volumes, Volume v,
                                            java.util.function.Consumer<List<Volume>> setter) {
        if (volumes == null) {
            setter.accept(new ArrayList<>(List.of(v)));
            return;
        }
        if (volumes.stream().noneMatch(x -> v.getName().equals(x.getName()))) {
            volumes.add(v);
        }
    }

    /** Proxy client cert Secret name — explicit spec.proxy.tls override, else operator default. */
    static String resolveClientCertSecret(KafkaCluster cr) {
        KafkaProxyTlsConfig tls = cr.getSpec().getProxy() != null
                ? cr.getSpec().getProxy().getTls() : null;
        if (tls != null && tls.getClientCertSecretRef() != null
                && !tls.getClientCertSecretRef().isBlank()) {
            return tls.getClientCertSecretRef();
        }
        return KafkaProxyOrchestrator.defaultClientCertSecret(KafkaProxyOrchestrator.PROXY_NAME);
    }

    private static EnvVar env(String name, String value) {
        return new EnvVarBuilder().withName(name).withValue(value).build();
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
