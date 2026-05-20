package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIServiceConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;

import static org.assertj.core.api.Assertions.assertThat;

class UIServiceBuilderTest {

    private final UIServiceBuilder builder = new UIServiceBuilder();

    @Test
    void build_nodePort_setsNodePortField() {
        Service svc = builder.build(ui("NodePort", 8080, 30808), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("NodePort");
        assertThat(svc.getSpec().getPorts().get(0).getPort()).isEqualTo(8080);
        assertThat(svc.getSpec().getPorts().get(0).getNodePort()).isEqualTo(30808);
        assertThat(svc.getMetadata().getOwnerReferences()).hasSize(1);
    }

    @Test
    void build_clusterIp_omitsNodePort() {
        Service svc = builder.build(ui("ClusterIP", 8080, 30808), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(svc.getSpec().getPorts().get(0).getNodePort()).isNull();
    }

    private KafkaUI ui(String type, int port, int nodePort) {
        KafkaUI ui = new KafkaUI();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-ui");
        meta.setNamespace("kafka");
        ui.setMetadata(meta);
        KafkaUISpec spec = new KafkaUISpec();
        KafkaUIServiceConfig s = new KafkaUIServiceConfig();
        s.setType(type);
        s.setPort(port);
        s.setNodePort(nodePort);
        spec.setService(s);
        ui.setSpec(spec);
        return ui;
    }

    private io.fabric8.kubernetes.api.model.OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withKind("KafkaUI").withName("kafka-ui").withUid("u").withController(true).build();
    }
}
