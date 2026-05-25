package se.afshin.yavari.kafka.editor.admin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.AdminClientFactory;
import se.afshin.yavari.kafka.editor.admin.AdminErrors;
import se.afshin.yavari.kafka.editor.admin.dto.GroupOffsets;
import se.afshin.yavari.kafka.editor.admin.dto.GroupSummary;
import se.afshin.yavari.kafka.editor.admin.dto.PartitionLag;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Consumer-group reads (with lag), offset resets and deletion. */
@ApplicationScoped
public class GroupService {

    @Inject
    AdminClientFactory factory;

    /** Every consumer group with its state and member count. */
    public List<GroupSummary> list(ConnectionConfig conn) {
        Admin admin = factory.admin(conn);
        Collection<ConsumerGroupListing> listings =
                AdminErrors.await(admin.listConsumerGroups().all());
        List<String> ids = listings.stream()
                .map(ConsumerGroupListing::groupId).toList();
        Map<String, ConsumerGroupDescription> described = ids.isEmpty()
                ? Map.of()
                : AdminErrors.await(admin.describeConsumerGroups(ids).all());

        List<GroupSummary> out = new ArrayList<>();
        for (ConsumerGroupListing listing : listings) {
            ConsumerGroupDescription d = described.get(listing.groupId());
            String state = d != null
                    ? d.state().name()
                    : listing.state().map(Enum::name).orElse("UNKNOWN");
            int members = d != null ? d.members().size() : 0;
            String assignor = d != null ? d.partitionAssignor() : "";
            String coordinator = d != null && d.coordinator() != null
                    ? d.coordinator().host() + ":" + d.coordinator().port()
                    : "";
            out.add(new GroupSummary(listing.groupId(), state, members,
                    assignor, coordinator));
        }
        out.sort(Comparator.comparing(GroupSummary::groupId));
        return out;
    }

    /** A group's committed offsets and per-partition lag. */
    public GroupOffsets offsets(ConnectionConfig conn, String groupId) {
        Admin admin = factory.admin(conn);
        Map<TopicPartition, OffsetAndMetadata> committed = AdminErrors.await(
                admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata());
        Map<TopicPartition, OffsetSpec> latestSpec = new HashMap<>();
        committed.keySet().forEach(tp -> latestSpec.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResultInfo> end = latestSpec.isEmpty()
                ? Map.of()
                : AdminErrors.await(admin.listOffsets(latestSpec).all());

        List<PartitionLag> partitions = new ArrayList<>();
        long total = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e
                : committed.entrySet()) {
            TopicPartition tp = e.getKey();
            Long committedOffset = e.getValue() == null
                    ? null : e.getValue().offset();
            long endOffset = end.containsKey(tp) ? end.get(tp).offset() : 0;
            Long lag = lagOf(committedOffset, endOffset);
            if (lag != null) {
                total += lag;
            }
            partitions.add(new PartitionLag(tp.topic(), tp.partition(),
                    committedOffset, endOffset, lag));
        }
        partitions.sort(Comparator.comparing(PartitionLag::topic)
                .thenComparingInt(PartitionLag::partition));
        return new GroupOffsets(groupId, groupState(admin, groupId), total,
                partitions);
    }

    /** Reset a group's offsets for a topic (one partition, or all of them). */
    public void resetOffsets(ConnectionConfig conn, String groupId, String topic,
            Integer partition, String target, Long offset, Long timestamp) {
        if (topic == null || topic.isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST", "A topic is required.");
        }
        Admin admin = factory.admin(conn);
        String state = groupState(admin, groupId);
        if (!"EMPTY".equals(state) && !"DEAD".equals(state)) {
            throw new AdminApiException(409, "CONFLICT", "The group is " + state
                    + " — stop its consumers before resetting offsets.");
        }
        List<TopicPartition> tps = resolvePartitions(admin, topic, partition);
        AdminErrors.await(admin.alterConsumerGroupOffsets(groupId,
                resolveOffsets(admin, tps, target, offset, timestamp)).all());
    }

    /** Delete a consumer group. */
    public void delete(ConnectionConfig conn, String groupId) {
        AdminErrors.await(
                factory.admin(conn).deleteConsumerGroups(List.of(groupId)).all());
    }

    /** Lag = end - committed, clamped at 0; null when there is no commit. */
    static Long lagOf(Long committed, long end) {
        if (committed == null) {
            return null;
        }
        long lag = end - committed;
        return lag < 0 ? 0L : lag;
    }

    private String groupState(Admin admin, String groupId) {
        ConsumerGroupDescription d = AdminErrors.await(
                admin.describeConsumerGroups(List.of(groupId)).all())
                .get(groupId);
        return d != null ? d.state().name() : "UNKNOWN";
    }

    private List<TopicPartition> resolvePartitions(Admin admin, String topic,
            Integer partition) {
        if (partition != null) {
            return List.of(new TopicPartition(topic, partition));
        }
        TopicDescription td = AdminErrors.await(
                admin.describeTopics(List.of(topic)).allTopicNames())
                .get(topic);
        if (td == null) {
            throw new AdminApiException(404, "NOT_FOUND",
                    "No such topic: " + topic);
        }
        return td.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
    }

    private Map<TopicPartition, OffsetAndMetadata> resolveOffsets(Admin admin,
            List<TopicPartition> tps, String target, Long explicitOffset,
            Long timestamp) {
        String mode = target == null ? "" : target.toUpperCase();
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        if ("OFFSET".equals(mode)) {
            if (explicitOffset == null || explicitOffset < 0) {
                throw new AdminApiException(400, "BAD_REQUEST",
                        "A non-negative offset is required.");
            }
            tps.forEach(tp -> out.put(tp, new OffsetAndMetadata(explicitOffset)));
            return out;
        }
        OffsetSpec spec = switch (mode) {
            case "EARLIEST" -> OffsetSpec.earliest();
            case "LATEST" -> OffsetSpec.latest();
            case "TIMESTAMP" -> {
                if (timestamp == null) {
                    throw new AdminApiException(400, "BAD_REQUEST",
                            "A timestamp is required.");
                }
                yield OffsetSpec.forTimestamp(timestamp);
            }
            default -> throw new AdminApiException(400, "BAD_REQUEST",
                    "Unknown reset target: " + target);
        };
        Map<TopicPartition, OffsetSpec> query = new HashMap<>();
        tps.forEach(tp -> query.put(tp, spec));
        Map<TopicPartition, ListOffsetsResultInfo> resolved =
                AdminErrors.await(admin.listOffsets(query).all());
        for (TopicPartition tp : tps) {
            ListOffsetsResultInfo info = resolved.get(tp);
            long resolvedOffset = info != null && info.offset() >= 0
                    ? info.offset() : 0;
            out.put(tp, new OffsetAndMetadata(resolvedOffset));
        }
        return out;
    }
}
