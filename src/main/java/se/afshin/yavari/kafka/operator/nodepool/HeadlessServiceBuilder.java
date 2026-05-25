package se.afshin.yavari.kafka.operator.nodepool;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import se.afshin.yavari.kafka.operator.crd.KafkaListenerSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.infra.OwnerReferences;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class HeadlessServiceBuilder {

    private static final int BROKER_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;

    @ConfigProperty(name = "kafka.networking.mcs-enabled")
    boolean mcsEnabled;

    public Service build(KafkaNodePool pool, String namespace, String clusterName,
                         boolean isController, boolean isBroker, List<KafkaListenerSpec> listeners) {
        List<ServicePort> ports = new ArrayList<>();
        if (isBroker) {
            ports.add(new ServicePortBuilder()
                    .withName("kafka").withPort(BROKER_PORT).withTargetPort(new IntOrString(BROKER_PORT)).build());
        }
        if (isController) {
            ports.add(new ServicePortBuilder()
                    .withName("controller").withPort(CONTROLLER_PORT).withTargetPort(new IntOrString(CONTROLLER_PORT)).build());
        }
        if (isBroker && listeners != null) {
            for (KafkaListenerSpec l : listeners) {
                String portName = l.getName().toLowerCase().replace('_', '-');
                ports.add(new ServicePortBuilder()
                        .withName(portName).withPort(l.getPort()).withTargetPort(new IntOrString(l.getPort())).build());
            }
        }

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(pool.getMetadata().getName() + "-headless")
                    .withNamespace(namespace)
                    .withLabels(Map.of(
                        KafkaPodSet.CLUSTER_LABEL,    clusterName,
                        KafkaPodSet.NODE_POOL_LABEL,  pool.getMetadata().getName(),
                        KafkaPodSet.MANAGED_BY_LABEL, KafkaPodSet.MANAGED_BY_VALUE
                    ))
                    .withOwnerReferences(OwnerReferences.singleton(pool))
                .endMetadata()
                .withNewSpec()
                    .withClusterIP("None")
                    .withSelector(Map.of(KafkaPodSet.NODE_POOL_LABEL, pool.getMetadata().getName()))
                    .withPorts(ports)
                    .withPublishNotReadyAddresses(true)
                .endSpec()
                .build();
    }

    public Optional<GenericKubernetesResource> buildServiceExport(String name, String namespace, KafkaNodePool pool) {
        if (!mcsEnabled) {
            return Optional.empty();
        }
        GenericKubernetesResource export = new GenericKubernetesResource();
        export.setApiVersion("multicluster.x-k8s.io/v1alpha1");
        export.setKind("ServiceExport");
        export.setMetadata(new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace)
                .withOwnerReferences(OwnerReferences.singleton(pool))
                .build());
        return Optional.of(export);
    }
}
