package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Focused unit test for the cross-cluster gate's "should this trigger a roll?" decision.
 * The full orchestrator path is exercised via the e2e cert-rotation drill — this test pins
 * the small helper that decides whether an applied Deployment will roll its pods, which is
 * what gates {@code rollTracker.markRolling} and the {@code clusterRollOrder} defer.
 */
class ApicurioOrchestratorTest {

    private static final String IMAGE = "quay.io/apicurio/apicurio-registry:3.0.6";
    private static final String HASH_OLD = "aabbccdd00112233";
    private static final String HASH_NEW = "11223344aabbccdd";

    @Test
    void rollWillHappen_existingNull_returnsTrue() {
        assertThat(ApicurioOrchestrator.rollWillHappen(null, IMAGE, HASH_NEW)).isTrue();
    }

    @Test
    void rollWillHappen_sameImageSameHash_returnsFalse() {
        Deployment dep = deployment(IMAGE, HASH_OLD);
        assertThat(ApicurioOrchestrator.rollWillHappen(dep, IMAGE, HASH_OLD)).isFalse();
    }

    @Test
    void rollWillHappen_imageChanged_returnsTrue() {
        Deployment dep = deployment(IMAGE, HASH_OLD);
        assertThat(ApicurioOrchestrator.rollWillHappen(dep, IMAGE + "-next", HASH_OLD)).isTrue();
    }

    @Test
    void rollWillHappen_hashChanged_returnsTrue() {
        // A cert rotation flows in through configHash even when the image is unchanged.
        Deployment dep = deployment(IMAGE, HASH_OLD);
        assertThat(ApicurioOrchestrator.rollWillHappen(dep, IMAGE, HASH_NEW)).isTrue();
    }

    @Test
    void rollWillHappen_nullDesiredImage_treatsAsNoImageChange() {
        // When spec.apicurio.image is unset (operator-defaulted), a null desired image
        // must not force a roll on every reconcile — the configHash carries the real signal.
        Deployment dep = deployment(IMAGE, HASH_OLD);
        assertThat(ApicurioOrchestrator.rollWillHappen(dep, null, HASH_OLD)).isFalse();
    }

    private Deployment deployment(String image, String configHash) {
        Container c = new ContainerBuilder().withName("registry").withImage(image).build();
        PodSpec ps = new PodSpecBuilder().withContainers(c).build();
        PodTemplateSpec template = new PodTemplateSpecBuilder()
                .withNewMetadata()
                    .withAnnotations(Map.of(ApicurioDeploymentBuilder.CONFIG_HASH_ANNOTATION, configHash))
                .endMetadata()
                .withSpec(ps)
                .build();
        return new DeploymentBuilder()
                .withNewMetadata().withName("apicurio-registry").endMetadata()
                .withNewSpec().withTemplate(template).endSpec()
                .build();
    }
}
