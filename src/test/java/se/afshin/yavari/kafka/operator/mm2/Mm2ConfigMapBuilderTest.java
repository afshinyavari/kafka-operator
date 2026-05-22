package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;

import static org.assertj.core.api.Assertions.assertThat;

class Mm2ConfigMapBuilderTest {

    private final Mm2ConfigMapBuilder builder = new Mm2ConfigMapBuilder();

    private MirrorMaker2 cr() {
        MirrorMaker2 cr = new MirrorMaker2();
        cr.setMetadata(new ObjectMetaBuilder().withName("my-mm2").withNamespace("kafka").build());
        cr.setSpec(new MirrorMaker2Spec());
        return cr;
    }

    private OwnerReference owner() {
        return new OwnerReferenceBuilder().withName("my-mm2").withUid("u")
                .withKind("MirrorMaker2").withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withController(true).build();
    }

    @Test
    void includesJmxConfigKeyWhenProvided() {
        ConfigMap cm = builder.build(cr(), "props", "jmx-yaml", owner());
        assertThat(cm.getData()).containsEntry("jmx-config.yaml", "jmx-yaml");
        assertThat(cm.getData()).containsEntry("mm2.properties", "props");
    }

    @Test
    void omitsJmxConfigKeyWhenNull() {
        ConfigMap cm = builder.build(cr(), "props", null, owner());
        assertThat(cm.getData()).doesNotContainKey("jmx-config.yaml");
        assertThat(cm.getData()).containsEntry("mm2.properties", "props");
    }
}
