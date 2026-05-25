package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.endpoint.ResolvedKafkaEndpoint;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Mm2DeploymentBuilderTest {

    private final Mm2DeploymentBuilder builder = new Mm2DeploymentBuilder();

    private MirrorMaker2 cr() {
        MirrorMaker2 cr = new MirrorMaker2();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-mm2").withNamespace("kafka").build());
        cr.setSpec(new MirrorMaker2Spec());
        cr.getSpec().setImage("mm2:dev");
        return cr;
    }

    private OwnerReference owner() {
        return new OwnerReferenceBuilder().withName("my-mm2").withUid("u")
                .withKind("MirrorMaker2").withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withController(true).build();
    }

    @Test
    void noTlsNoSaslMinimal() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 1, "hash1", false, owner());

        assertThat(dep.getMetadata().getName()).isEqualTo("my-mm2");
        assertThat(dep.getSpec().getReplicas()).isEqualTo(1);
        var pod = dep.getSpec().getTemplate().getSpec();
        assertThat(pod.getInitContainers()).isEmpty();
        assertThat(pod.getContainers()).hasSize(1);
        Container worker = pod.getContainers().get(0);
        assertThat(worker.getCommand()).containsExactly("/opt/kafka/bin/connect-mirror-maker.sh");
        assertThat(worker.getArgs()).containsExactly("/etc/mm2/mm2.properties");
        // configHash annotation
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations())
                .containsEntry(Mm2DeploymentBuilder.CONFIG_HASH_ANNOTATION, "hash1");
        // topology spread for MCS
        assertThat(pod.getTopologySpreadConstraints()).hasSize(1);
    }

    @Test
    void tlsAddsPerSideInitContainersAndMounts() {
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint("b:9094", "src-tls", null, null, null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint("c:9094", "tgt-tls", null, null, null, false);
        Deployment dep = builder.build(cr(), src, tgt, 3, "h", false, owner());

        var pod = dep.getSpec().getTemplate().getSpec();
        assertThat(pod.getInitContainers()).extracting(Container::getName)
                .containsExactlyInAnyOrder("pem-to-pkcs12-source", "pem-to-pkcs12-target");
        // Worker mounts the converted pkcs12 dirs
        var worker = pod.getContainers().get(0);
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/pkcs12/source", "/etc/mm2/pkcs12/target");
    }

    @Test
    void saslAddsSecretMounts() {
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint("b:9093", null,
                new ResolvedKafkaEndpoint.Sasl("PLAIN", "src-creds"), null, null, false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint("c:9094", "tgt-tls", null, null, null, false);
        Deployment dep = builder.build(cr(), src, tgt, 1, "h", false, owner());

        var worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/sasl/source");
    }

    @Test
    void schemaRegistryAuthAddsMount() {
        ResolvedKafkaEndpoint src = new ResolvedKafkaEndpoint("b:9094", "src-tls", null,
                "http://reg.src", "src-reg-auth", false);
        ResolvedKafkaEndpoint tgt = new ResolvedKafkaEndpoint("c:9094", "tgt-tls", null,
                "http://reg.dst", "dst-reg-auth", false);
        Deployment dep = builder.build(cr(), src, tgt, 1, "h", false, owner());

        var worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/registry-auth/source", "/etc/mm2/registry-auth/target");
    }

    @Test
    void replicasArePropagated() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 3, "h", false, owner());
        assertThat(dep.getSpec().getReplicas()).isEqualTo(3);
    }

    @Test
    void metricsEnabledAttachesJavaagentPortAndMount() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 1, "h", true, owner());
        Container worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(worker.getEnv()).extracting(e -> e.getName())
                .contains("KAFKA_OPTS", "KAFKA_HEAP_OPTS");
        assertThat(worker.getEnv()).anySatisfy(e -> {
            if ("KAFKA_OPTS".equals(e.getName())) {
                assertThat(e.getValue())
                        .contains("-javaagent:/opt/jmx-exporter/jmx-exporter.jar=9101:")
                        .contains("/etc/mm2/jmx-config.yaml");
            }
        });
        assertThat(worker.getPorts()).extracting(p -> p.getName()).contains("metrics");
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/jmx-config.yaml");
    }

    @Test
    void metricsDisabledHasNoJavaagentNoPortNoMount() {
        ResolvedKafkaEndpoint ep = new ResolvedKafkaEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 1, "h", false, owner());
        Container worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(worker.getEnv()).extracting(e -> e.getName()).doesNotContain("KAFKA_OPTS");
        assertThat(worker.getPorts()).isNullOrEmpty();
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .doesNotContain("/etc/mm2/jmx-config.yaml");
    }
}
