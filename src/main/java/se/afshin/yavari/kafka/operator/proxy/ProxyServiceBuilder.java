package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;

import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class ProxyServiceBuilder {

    public Service build(KafkaProxy proxy, int brokerCount, String namespace) {
        String name = proxy.getMetadata().getName();
        int clientPort = proxy.getSpec().getClientPort();
        var labels = ProxyDeploymentBuilder.labels(name);

        // Total port count = one per node across all ranges, or local brokerCount if no ranges defined
        List<BrokerNodeIdRange> ranges = proxy.getSpec().getBrokerNodeIdRanges();
        int totalNodeCount = (ranges != null && !ranges.isEmpty())
                ? ranges.stream().mapToInt(r -> r.getEnd() - r.getStart() + 1).sum()
                : brokerCount;

        var ports = new ArrayList<io.fabric8.kubernetes.api.model.ServicePort>();
        ports.add(new ServicePortBuilder()
                .withName("bootstrap")
                .withPort(clientPort)
                .withTargetPort(new IntOrString(clientPort))
                .build());
        for (int i = 0; i < totalNodeCount; i++) {
            int brokerPort = clientPort + i + 1;
            ports.add(new ServicePortBuilder()
                    .withName("broker-" + i)
                    .withPort(brokerPort)
                    .withTargetPort(new IntOrString(brokerPort))
                    .build());
        }

        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                    .withSelector(labels)
                    .withPorts(ports)
                .endSpec()
                .build();
    }
}
