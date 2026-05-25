package se.afshin.yavari.kafka.operator.audit;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.AuditKafkaTopicSpec;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterAuditSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig;
import se.afshin.yavari.kafka.operator.topic.BrokerBootstrapResolver;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AuditOrchestratorTest {

    private static final String NS = "kafka";
    private final AuditOrchestrator orchestrator = new AuditOrchestrator();
    {
        // Stand-in for the CDI-injected resolver — returns a fixed broker pool address.
        orchestrator.bootstrapResolver = new BrokerBootstrapResolver() {
            @Override
            public String resolve(String clusterName, String namespace) {
                return "brokers-a-headless." + namespace + ".svc.cluster.local:9092";
            }
        };
    }

    @Test
    void kafkaSinkEnabled_offByDefault() {
        KafkaCluster cr = cluster(false, false, false);
        assertThat(AuditOrchestrator.kafkaSinkEnabled(cr)).isFalse();

        cr = cluster(true, false, false);   // audit set, but kafkaTopic null
        assertThat(AuditOrchestrator.kafkaSinkEnabled(cr)).isFalse();

        cr = cluster(true, true, false);    // audit + kafkaTopic but enabled=false
        assertThat(AuditOrchestrator.kafkaSinkEnabled(cr)).isFalse();

        cr = cluster(true, true, true);     // fully on
        assertThat(AuditOrchestrator.kafkaSinkEnabled(cr)).isTrue();
    }

    @Test
    void envVars_emptyWhenSinkDisabled() {
        assertThat(orchestrator.envVars(cluster(false, false, false), NS)).isEmpty();
    }

    @Test
    void envVars_populatedWhenSinkEnabled() {
        KafkaCluster cr = cluster(true, true, true);
        List<EnvVar> env = orchestrator.envVars(cr, NS);
        assertThat(env).hasSize(5);
        assertThat(envValue(env, "KAFKA_AUDIT_TOPIC")).isEqualTo("__audit");
        // Bootstrap is resolved via BrokerBootstrapResolver (broker pool headless on
        // cluster.local). Audit traffic goes direct to brokers, never via Kroxylicious.
        assertThat(envValue(env, "KAFKA_AUDIT_BOOTSTRAP"))
                .isEqualTo("brokers-a-headless.kafka.svc.cluster.local:9092");
        assertThat(envValue(env, "KAFKA_AUDIT_TLS_CERT")).isEqualTo("/etc/audit-tls/tls.crt");
        assertThat(envValue(env, "KAFKA_AUDIT_TLS_KEY")).isEqualTo("/etc/audit-tls/tls.key");
        assertThat(envValue(env, "KAFKA_AUDIT_TLS_CA")).isEqualTo("/etc/audit-tls/ca.crt");
    }

    @Test
    void tlsVolume_defaultsToProxyClientCertSecret() {
        KafkaCluster cr = cluster(true, true, true);
        assertThat(orchestrator.tlsVolume(cr).getSecret().getSecretName())
                .isEqualTo("kafka-proxy-client-tls");
    }

    @Test
    void tlsVolume_honoursProxyTlsOverride() {
        KafkaCluster cr = cluster(true, true, true);
        KafkaProxyTlsConfig tls = new KafkaProxyTlsConfig();
        tls.setClientCertSecretRef("my-overridden-secret");
        cr.getSpec().getProxy().setTls(tls);
        assertThat(orchestrator.tlsVolume(cr).getSecret().getSecretName())
                .isEqualTo("my-overridden-secret");
    }

    @Test
    void injectIntoDeployment_addsVolumeAndContainerEnv() {
        KafkaCluster cr = cluster(true, true, true);
        Deployment dep = sampleDeployment();

        orchestrator.injectIntoDeployment(dep, cr, NS);

        var podSpec = dep.getSpec().getTemplate().getSpec();
        assertThat(podSpec.getVolumes()).anySatisfy(v ->
                assertThat(v.getName()).isEqualTo(AuditOrchestrator.AUDIT_TLS_VOLUME));
        var container = podSpec.getContainers().get(0);
        assertThat(container.getEnv()).extracting(EnvVar::getName)
                .contains("KAFKA_AUDIT_BOOTSTRAP", "KAFKA_AUDIT_TOPIC");
        assertThat(container.getVolumeMounts()).anySatisfy(m ->
                assertThat(m.getName()).isEqualTo(AuditOrchestrator.AUDIT_TLS_VOLUME));
    }

    @Test
    void injectIntoDeployment_isIdempotent() {
        KafkaCluster cr = cluster(true, true, true);
        Deployment dep = sampleDeployment();

        orchestrator.injectIntoDeployment(dep, cr, NS);
        orchestrator.injectIntoDeployment(dep, cr, NS);

        var podSpec = dep.getSpec().getTemplate().getSpec();
        // Volume appears exactly once even after two reconciles.
        long volCount = podSpec.getVolumes().stream()
                .filter(v -> AuditOrchestrator.AUDIT_TLS_VOLUME.equals(v.getName()))
                .count();
        assertThat(volCount).isEqualTo(1);
        long envCount = podSpec.getContainers().get(0).getEnv().stream()
                .filter(e -> "KAFKA_AUDIT_BOOTSTRAP".equals(e.getName())).count();
        assertThat(envCount).isEqualTo(1);
    }

    @Test
    void injectIntoDeployment_noOpWhenSinkDisabled() {
        KafkaCluster cr = cluster(false, false, false);
        Deployment dep = sampleDeployment();

        orchestrator.injectIntoDeployment(dep, cr, NS);

        var podSpec = dep.getSpec().getTemplate().getSpec();
        assertThat(podSpec.getVolumes()).noneSatisfy(v ->
                assertThat(v.getName()).isEqualTo(AuditOrchestrator.AUDIT_TLS_VOLUME));
        assertThat(podSpec.getContainers().get(0).getEnv()).extracting(EnvVar::getName)
                .doesNotContain("KAFKA_AUDIT_BOOTSTRAP");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static KafkaCluster cluster(boolean withAudit, boolean withKafkaTopic, boolean enabled) {
        KafkaCluster cr = new KafkaCluster();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-cluster").withNamespace(NS).build());
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(List.of(entry("A")));
        spec.setProxy(new KafkaClusterProxySpec());
        if (withAudit) {
            KafkaClusterAuditSpec audit = new KafkaClusterAuditSpec();
            if (withKafkaTopic) {
                AuditKafkaTopicSpec sink = new AuditKafkaTopicSpec();
                sink.setEnabled(enabled);
                audit.setKafkaTopic(sink);
            }
            spec.setAudit(audit);
        }
        cr.setSpec(spec);
        return cr;
    }

    private static ClusterEntry entry(String id) {
        ClusterEntry e = new ClusterEntry();
        e.setId(id);
        return e;
    }

    private static Deployment sampleDeployment() {
        return new DeploymentBuilder()
                .withNewMetadata().withName("kroxylicious").withNamespace(NS).endMetadata()
                .withNewSpec()
                    .withNewTemplate()
                        .withNewSpec()
                            .withContainers(new ContainerBuilder()
                                    .withName("kroxylicious")
                                    .withImage("kroxylicious:0.21.0")
                                    .withEnv(new EnvVarBuilder().withName("EXISTING").withValue("v").build())
                                    .withVolumeMounts(new VolumeMountBuilder()
                                            .withName("existing").withMountPath("/x").build())
                                    .build())
                            .withVolumes(new VolumeBuilder().withName("existing").withNewEmptyDir().endEmptyDir().build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    private static String envValue(List<EnvVar> env, String name) {
        return env.stream().filter(e -> name.equals(e.getName())).findFirst()
                .map(EnvVar::getValue).orElse(null);
    }

    @SuppressWarnings("unused")
    private static List<EnvVar> mutableEnvList() { return new ArrayList<>(); }
}
