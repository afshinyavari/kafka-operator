package se.afshin.yavari.kafka.operator.mm2;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterRef;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2;
import se.afshin.yavari.kafka.operator.crd.MirrorMaker2Spec;
import se.afshin.yavari.kafka.operator.crd.Mm2Endpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2ExternalEndpoint;
import se.afshin.yavari.kafka.operator.crd.Mm2SchemaRegistryRef;
import se.afshin.yavari.kafka.operator.crd.SchemaRegistryType;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the reconciler's pure validation helpers without touching the kube client. */
class MirrorMaker2ReconcilerTest {

    private final MirrorMaker2Reconciler reconciler = new MirrorMaker2Reconciler();

    private MirrorMaker2 cr(Mm2Endpoint source, Mm2Endpoint target) {
        MirrorMaker2 cr = new MirrorMaker2();
        cr.setMetadata(new ObjectMetaBuilder().withName("mm2").withNamespace("kafka").build());
        MirrorMaker2Spec spec = new MirrorMaker2Spec();
        spec.setSource(source);
        spec.setTarget(target);
        cr.setSpec(spec);
        return cr;
    }

    private Mm2Endpoint managed(String name) {
        Mm2Endpoint ep = new Mm2Endpoint();
        KafkaClusterRef r = new KafkaClusterRef();
        r.setName(name);
        ep.setKafkaClusterRef(r);
        return ep;
    }

    private Mm2Endpoint external(String bootstrap) {
        Mm2Endpoint ep = new Mm2Endpoint();
        Mm2ExternalEndpoint x = new Mm2ExternalEndpoint();
        x.setBootstrap(bootstrap);
        ep.setExternal(x);
        return ep;
    }

    private Mm2Endpoint externalWithConfluent(String bootstrap) {
        Mm2Endpoint ep = external(bootstrap);
        Mm2SchemaRegistryRef sr = new Mm2SchemaRegistryRef();
        sr.setUrl("https://reg");
        sr.setType(SchemaRegistryType.CONFLUENT);
        ep.getExternal().setSchemaRegistry(sr);
        return ep;
    }

    private String invokeValidate(MirrorMaker2 cr) throws Exception {
        Method m = MirrorMaker2Reconciler.class.getDeclaredMethod("validate", MirrorMaker2.class);
        m.setAccessible(true);
        return (String) m.invoke(reconciler, cr);
    }

    @Test
    void validateAcceptsManagedToManaged() throws Exception {
        assertThat(invokeValidate(cr(managed("src"), managed("dst")))).isNull();
    }

    @Test
    void validateAcceptsManagedToExternal() throws Exception {
        assertThat(invokeValidate(cr(managed("src"), external("ext:9092")))).isNull();
    }

    @Test
    void validateAcceptsExternalToManaged() throws Exception {
        assertThat(invokeValidate(cr(external("ext:9092"), managed("dst")))).isNull();
    }

    @Test
    void validateRejectsBothExternal() throws Exception {
        assertThat(invokeValidate(cr(external("a:9092"), external("b:9092"))))
                .contains("at least one");
    }

    @Test
    void validateRejectsMissingSourceTarget() throws Exception {
        MirrorMaker2 cr = cr(managed("src"), null);
        assertThat(invokeValidate(cr)).contains("spec.target");
    }

    @Test
    void validateRejectsConfluentSchemaRegistry() throws Exception {
        MirrorMaker2 cr = cr(externalWithConfluent("ext:9092"), managed("dst"));
        assertThat(invokeValidate(cr)).contains("CONFLUENT");
    }

    @Test
    void rollWillHappenWhenImageChanges() {
        io.fabric8.kubernetes.api.model.apps.Deployment existing =
                new io.fabric8.kubernetes.api.model.apps.DeploymentBuilder()
                        .withNewSpec()
                            .withNewTemplate()
                                .withNewMetadata()
                                    .addToAnnotations(Mm2DeploymentBuilder.CONFIG_HASH_ANNOTATION, "h1")
                                .endMetadata()
                                .withNewSpec()
                                    .addNewContainer().withImage("mm2:old").endContainer()
                                .endSpec()
                            .endTemplate()
                        .endSpec()
                        .build();
        assertThat(MirrorMaker2Reconciler.rollWillHappen(existing, "mm2:new", "h1")).isTrue();
        assertThat(MirrorMaker2Reconciler.rollWillHappen(existing, "mm2:old", "h1")).isFalse();
        assertThat(MirrorMaker2Reconciler.rollWillHappen(existing, "mm2:old", "h2")).isTrue();
        assertThat(MirrorMaker2Reconciler.rollWillHappen(null, "mm2", "h")).isTrue();
    }
}
