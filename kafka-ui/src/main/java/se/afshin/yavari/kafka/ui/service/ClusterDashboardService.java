package se.afshin.yavari.kafka.ui.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class ClusterDashboardService {

    @Inject KafkaClientProvider clients;

    public record Dashboard(String clusterId, String kafkaClusterId, BrokerInfo controller, List<BrokerInfo> brokers) {}

    public record BrokerInfo(int id, String host, int port, String rack) {}

    public Dashboard load(String clusterId) throws ExecutionException, InterruptedException {
        AdminClient admin = clients.admin(clusterId);
        DescribeClusterResult dc = admin.describeCluster();
        Node controller = dc.controller().get();
        String kafkaId = dc.clusterId().get();

        List<BrokerInfo> brokers = new ArrayList<>();
        for (Node n : dc.nodes().get()) {
            brokers.add(new BrokerInfo(n.id(), n.host(), n.port(), n.rack()));
        }
        brokers.sort(Comparator.comparingInt(BrokerInfo::id));

        BrokerInfo ctrl = controller == null ? null
                : new BrokerInfo(controller.id(), controller.host(), controller.port(), controller.rack());
        return new Dashboard(clusterId, kafkaId, ctrl, brokers);
    }
}
