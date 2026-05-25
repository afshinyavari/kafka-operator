package se.afshin.yavari.kafka.editor.admin.service;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.AdminErrors;
import se.afshin.yavari.kafka.editor.admin.dto.BrokerInfo;
import se.afshin.yavari.kafka.editor.admin.dto.ClusterOverview;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

/** Cluster-level reads: brokers, controller, cluster id, aggregate counts. */
@ApplicationScoped
public class ClusterService {

    @Inject
    AdminClientFactory factory;

    public ClusterOverview overview(ConnectionConfig connection) {
        Admin admin = factory.admin(connection);

        DescribeClusterResult cluster = admin.describeCluster();
        Collection<Node> nodes = AdminErrors.await(cluster.nodes());
        Node controller = AdminErrors.await(cluster.controller());
        String clusterId = AdminErrors.await(cluster.clusterId());

        List<BrokerInfo> brokers = nodes.stream()
                .map(ClusterService::toBroker)
                .sorted(Comparator.comparingInt(BrokerInfo::id))
                .toList();

        Set<String> topics = AdminErrors.await(
                admin.listTopics(new ListTopicsOptions().listInternal(true)).names());
        Map<String, TopicDescription> described = AdminErrors.await(
                admin.describeTopics(topics).allTopicNames());
        int partitionCount = described.values().stream()
                .mapToInt(td -> td.partitions().size())
                .sum();

        return new ClusterOverview(
                clusterId,
                controller == null ? null : toBroker(controller),
                brokers,
                topics.size(),
                partitionCount);
    }

    private static BrokerInfo toBroker(Node node) {
        return new BrokerInfo(node.id(), node.host(), node.port(), node.rack());
    }
}
