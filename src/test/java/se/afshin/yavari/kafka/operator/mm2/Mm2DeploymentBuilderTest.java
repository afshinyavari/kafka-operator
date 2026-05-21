package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;

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
        ResolvedEndpoint ep = new ResolvedEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 1, "hash1", owner());

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
        ResolvedEndpoint src = new ResolvedEndpoint("b:9094", "src-tls", null, null, null, false);
        ResolvedEndpoint tgt = new ResolvedEndpoint("c:9094", "tgt-tls", null, null, null, false);
        Deployment dep = builder.build(cr(), src, tgt, 3, "h", owner());

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
        ResolvedEndpoint src = new ResolvedEndpoint("b:9093", null,
                new ResolvedEndpoint.Mm2Sasl("PLAIN", "src-creds"), null, null, false);
        ResolvedEndpoint tgt = new ResolvedEndpoint("c:9094", "tgt-tls", null, null, null, false);
        Deployment dep = builder.build(cr(), src, tgt, 1, "h", owner());

        var worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/sasl/source");
    }

    @Test
    void schemaRegistryAuthAddsMount() {
        ResolvedEndpoint src = new ResolvedEndpoint("b:9094", "src-tls", null,
                "http://reg.src", "src-reg-auth", false);
        ResolvedEndpoint tgt = new ResolvedEndpoint("c:9094", "tgt-tls", null,
                "http://reg.dst", "dst-reg-auth", false);
        Deployment dep = builder.build(cr(), src, tgt, 1, "h", owner());

        var worker = dep.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertThat(worker.getVolumeMounts()).extracting(v -> v.getMountPath())
                .contains("/etc/mm2/registry-auth/source", "/etc/mm2/registry-auth/target");
    }

    @Test
    void replicasArePropagated() {
        ResolvedEndpoint ep = new ResolvedEndpoint("b:9092", null, null, null, null, false);
        Deployment dep = builder.build(cr(), ep, ep, 3, "h", owner());
        assertThat(dep.getSpec().getReplicas()).isEqualTo(3);
    }
}
