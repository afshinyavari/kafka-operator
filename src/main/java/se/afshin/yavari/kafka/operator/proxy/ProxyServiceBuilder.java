package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ProxyServiceBuilder {

    public Service build(KafkaProxy proxy, int brokerCount, String namespace) {
        return build(proxy, brokerCount, namespace, ExternalAccessResolution.internal());
    }

    public Service build(KafkaProxy proxy, int brokerCount, String namespace,
                         ExternalAccessResolution external) {
        String name = proxy.getMetadata().getName();
        int clientPort = proxy.getSpec().getClientPort();
        var labels = ProxyDeploymentBuilder.labels(name);

        // Total port count = one per node across all ranges, or local brokerCount if no ranges defined
        List<BrokerNodeIdRange> ranges = proxy.getSpec().getBrokerNodeIdRanges();
        int totalNodeCount = (ranges != null && !ranges.isEmpty())
                ? ranges.stream().mapToInt(r -> r.getEnd() - r.getStart() + 1).sum()
                : brokerCount;

        ExternalAccessType type = external.type();
        // SNI-based routing (Gateway/Ingress) uses a single listen port and the proxy dispatches
        // by SNI hostname; port-based routing (default & LoadBalancer) uses one port per broker.
        boolean snglPort = type == ExternalAccessType.GATEWAY || type == ExternalAccessType.INGRESS;

        var ports = new ArrayList<ServicePort>();
        ports.add(new ServicePortBuilder()
                .withName("bootstrap")
                .withPort(clientPort)
                .withTargetPort(new IntOrString(clientPort))
                .build());
        if (!snglPort) {
            for (int i = 0; i < totalNodeCount; i++) {
                int brokerPort = clientPort + i + 1;
                ports.add(new ServicePortBuilder()
                        .withName("broker-" + i)
                        .withPort(brokerPort)
                        .withTargetPort(new IntOrString(brokerPort))
                        .build());
            }
        }

        var specBuilder = new ServiceSpecBuilder()
                .withSelector(labels)
                .withPorts(ports);

        if (type == ExternalAccessType.LOADBALANCER) {
            specBuilder.withType("LoadBalancer");
        }
        // GATEWAY / INGRESS / null all leave Service type at the default (ClusterIP).

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withSpec(specBuilder.build())
                .build();
    }
}
