package se.afshin.yavari.kafka.editor.admin.service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.AdminErrors;
import se.afshin.yavari.kafka.editor.admin.dto.ConfigKv;
import se.afshin.yavari.kafka.editor.admin.dto.PartitionDetail;
import se.afshin.yavari.kafka.editor.admin.dto.PartitionMetric;
import se.afshin.yavari.kafka.editor.admin.dto.TopicDetail;
import se.afshin.yavari.kafka.editor.admin.dto.TopicMetrics;
import se.afshin.yavari.kafka.editor.admin.dto.TopicSummary;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

/** Topic reads, writes (create / alter / delete / add-partitions) and metrics. */
@ApplicationScoped
public class TopicAdminService {

    /** Kafka's own topic-name rule: letters, digits, dot, underscore, hyphen. */
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]+");

    @Inject
    AdminClientFactory factory;

    // ── reads ─────────────────────────────────────────────────────────────

    /** Every topic on the cluster, internal ones included, sorted by name. */
    public List<TopicSummary> list(ConnectionConfig connection) {
        Admin admin = factory.admin(connection);
        Set<String> names = AdminErrors.await(
                admin.listTopics(new ListTopicsOptions().listInternal(true)).names());
        Map<String, TopicDescription> described = AdminErrors.await(
                admin.describeTopics(names).allTopicNames());

        List<TopicSummary> out = new ArrayList<>(described.size());
        for (TopicDescription td : described.values()) {
            int partitions = td.partitions().size();
            int rf = partitions == 0 ? 0 : td.partitions().get(0).replicas().size();
            out.add(new TopicSummary(td.name(), partitions, rf, td.isInternal()));
        }
        out.sort(Comparator.comparing(TopicSummary::name));
        return out;
    }

    /** Partitions and configuration for one topic. */
    public TopicDetail describe(ConnectionConfig connection, String topic) {
        Admin admin = factory.admin(connection);
        TopicDescription td = describeOne(admin, topic);
        List<PartitionDetail> partitions = td.partitions().stream()
                .map(p -> new PartitionDetail(
                        p.partition(),
                        p.leader() == null ? -1 : p.leader().id(),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();
        return new TopicDetail(td.name(), td.isInternal(), partitions,
                describeConfigs(admin, topic));
    }

    /** Offset-based counts, best-effort size and health for one topic. */
    public TopicMetrics metrics(ConnectionConfig connection, String topic) {
        Admin admin = factory.admin(connection);
        TopicDescription td = describeOne(admin, topic);

        List<TopicPartition> tps = td.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
        Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (TopicPartition tp : tps) {
            earliest.put(tp, OffsetSpec.earliest());
            latest.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResultInfo> begin =
                AdminErrors.await(admin.listOffsets(earliest).all());
        Map<TopicPartition, ListOffsetsResultInfo> end =
                AdminErrors.await(admin.listOffsets(latest).all());

        List<PartitionMetric> perPartition = new ArrayList<>();
        long total = 0;
        int underReplicated = 0;
        int offline = 0;
        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            long start = offsetOf(begin.get(tp));
            long endOffset = offsetOf(end.get(tp));
            long count = Math.max(0, endOffset - start);
            total += count;
            if (p.leader() == null) offline++;
            if (p.isr().size() < p.replicas().size()) underReplicated++;
            perPartition.add(new PartitionMetric(p.partition(), start, endOffset, count));
        }
        return new TopicMetrics(td.partitions().size(), total,
                topicSize(admin, new HashSet<>(tps)), underReplicated, offline,
                perPartition);
    }

    // ── writes ────────────────────────────────────────────────────────────

    /** Create a topic. */
    public void create(ConnectionConfig connection, String name, int partitions,
            short replicationFactor, Map<String, String> configs) {
        validateTopicName(name);
        if (partitions < 1) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "A topic needs at least one partition.");
        }
        if (replicationFactor < 1) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Replication factor must be at least 1.");
        }
        NewTopic topic = new NewTopic(name, partitions, replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            topic.configs(configs);
        }
        AdminErrors.await(
                factory.admin(connection).createTopics(List.of(topic)).all());
    }

    /**
     * Incrementally alter a topic's config. A blank value resets that key to
     * its default (a DELETE op); a non-blank value sets it.
     */
    public void alterConfigs(ConnectionConfig connection, String topic,
            Map<String, String> changes) {
        if (changes == null || changes.isEmpty()) {
            return;
        }
        List<AlterConfigOp> ops = new ArrayList<>();
        for (Map.Entry<String, String> change : changes.entrySet()) {
            String value = change.getValue();
            boolean reset = value == null || value.isBlank();
            ops.add(new AlterConfigOp(
                    new ConfigEntry(change.getKey(), reset ? "" : value),
                    reset ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET));
        }
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        AdminErrors.await(factory.admin(connection)
                .incrementalAlterConfigs(Map.of(resource, ops)).all());
    }

    /** Increase a topic's partition count to {@code totalCount}. */
    public void addPartitions(ConnectionConfig connection, String topic,
            int totalCount) {
        if (totalCount < 1) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Partition count must be positive.");
        }
        AdminErrors.await(factory.admin(connection)
                .createPartitions(Map.of(topic, NewPartitions.increaseTo(totalCount)))
                .all());
    }

    /** Delete a topic. */
    public void delete(ConnectionConfig connection, String topic) {
        AdminErrors.await(
                factory.admin(connection).deleteTopics(List.of(topic)).all());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    static void validateTopicName(String name) {
        if (name == null || name.isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Topic name is required.");
        }
        if (name.length() > 249) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Topic name must be 249 characters or fewer.");
        }
        if (name.equals(".") || name.equals("..")) {
            throw new AdminApiException(400, "BAD_REQUEST", "Invalid topic name.");
        }
        if (!TOPIC_NAME.matcher(name).matches()) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Topic name may only contain letters, digits, dot, "
                            + "underscore and hyphen.");
        }
    }

    private TopicDescription describeOne(Admin admin, String topic) {
        TopicDescription td = AdminErrors.await(
                admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        if (td == null) {
            throw new AdminApiException(404, "NOT_FOUND", "No such topic: " + topic);
        }
        return td;
    }

    /** Topic configs — empty (rather than failing) if the caller may not read them. */
    private List<ConfigKv> describeConfigs(Admin admin, String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try {
            Map<ConfigResource, Config> result = AdminErrors.await(
                    admin.describeConfigs(List.of(resource)).all());
            Config config = result.get(resource);
            if (config == null) {
                return List.of();
            }
            return config.entries().stream()
                    .map(e -> new ConfigKv(e.name(), e.value(),
                            e.isDefault(), e.isSensitive(), e.isReadOnly()))
                    .sorted(Comparator.comparing(ConfigKv::name))
                    .toList();
        } catch (AdminApiException e) {
            if ("AUTH".equals(e.kind())) {
                return List.of();
            }
            throw e;
        }
    }

    private static long offsetOf(ListOffsetsResultInfo info) {
        return info == null ? 0 : info.offset();
    }

    /** Sum of replica sizes for the topic's partitions — -1 if unavailable. */
    private long topicSize(Admin admin, Set<TopicPartition> partitions) {
        try {
            List<Integer> brokerIds = AdminErrors.await(admin.describeCluster().nodes())
                    .stream().map(Node::id).toList();
            Map<Integer, Map<String, LogDirDescription>> dirs =
                    AdminErrors.await(admin.describeLogDirs(brokerIds).allDescriptions());
            long size = 0;
            for (Map<String, LogDirDescription> perBroker : dirs.values()) {
                for (LogDirDescription dir : perBroker.values()) {
                    for (Map.Entry<TopicPartition, ?> replica
                            : dir.replicaInfos().entrySet()) {
                        if (partitions.contains(replica.getKey())) {
                            size += ((org.apache.kafka.clients.admin.ReplicaInfo)
                                    replica.getValue()).size();
                        }
                    }
                }
            }
            return size;
        } catch (Exception e) {
            return -1;
        }
    }
}
