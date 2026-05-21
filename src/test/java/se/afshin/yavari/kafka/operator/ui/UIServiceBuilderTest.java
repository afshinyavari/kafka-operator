package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;

import static org.assertj.core.api.Assertions.assertThat;

class UIServiceBuilderTest {

    private final UIServiceBuilder builder = new UIServiceBuilder();

    @Test
    void build_nodePort_setsNodePortField() {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.NODEPORT);
        ea.setNodePort(30808);

        Service svc = builder.build(ui(ea), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("NodePort");
        assertThat(svc.getSpec().getPorts().get(0).getPort()).isEqualTo(KafkaUISpec.PORT);
        assertThat(svc.getSpec().getPorts().get(0).getNodePort()).isEqualTo(30808);
        assertThat(svc.getMetadata().getOwnerReferences()).hasSize(1);
    }

    @Test
    void build_loadBalancer_emitsLoadBalancerType() {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.LOADBALANCER);

        Service svc = builder.build(ui(ea), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("LoadBalancer");
        assertThat(svc.getSpec().getPorts().get(0).getNodePort()).isNull();
    }

    @Test
    void build_gateway_stayClusterIp() {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.GATEWAY);

        Service svc = builder.build(ui(ea), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("ClusterIP");
        assertThat(svc.getSpec().getPorts().get(0).getNodePort()).isNull();
    }

    @Test
    void build_ingress_stayClusterIp() {
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.INGRESS);

        Service svc = builder.build(ui(ea), ownerRef());
        assertThat(svc.getSpec().getType()).isEqualTo("ClusterIP");
    }

    private KafkaUI ui(HttpExternalAccessConfig ea) {
        KafkaUI ui = new KafkaUI();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-ui");
        meta.setNamespace("kafka");
        ui.setMetadata(meta);
        KafkaUISpec spec = new KafkaUISpec();
        spec.setExternalAccess(ea);
        ui.setSpec(spec);
        return ui;
    }

    private io.fabric8.kubernetes.api.model.OwnerReference ownerRef() {
        return new OwnerReferenceBuilder()
                .withApiVersion("kafka.yavari.afshin.se/v1alpha1")
                .withKind("KafkaUI").withName("kafka-ui").withUid("u").withController(true).build();
    }
}
