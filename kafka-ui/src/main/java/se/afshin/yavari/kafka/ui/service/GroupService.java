package se.afshin.yavari.kafka.ui.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class GroupService {

    @Inject KafkaClientProvider clients;

    public enum ResetTarget { EARLIEST, LATEST, OFFSET }

    public record GroupSummary(String id, String state, int members, String partitionAssignor) {}

    public List<GroupSummary> list(String clusterId) throws ExecutionException, InterruptedException {
        AdminClient admin = clients.admin(clusterId);
        Collection<ConsumerGroupListing> all = admin.listConsumerGroups().all().get();
        List<String> ids = all.stream().map(ConsumerGroupListing::groupId).toList();
        if (ids.isEmpty()) return List.of();

        var descs = admin.describeConsumerGroups(ids).all().get();
        List<GroupSummary> out = new ArrayList<>(descs.size());
        for (ConsumerGroupDescription d : descs.values()) {
            out.add(new GroupSummary(
                    d.groupId(),
                    d.state() == null ? "UNKNOWN" : d.state().toString(),
                    d.members() == null ? 0 : d.members().size(),
                    d.partitionAssignor() == null ? "" : d.partitionAssignor()));
        }
        out.sort(Comparator.comparing(GroupSummary::id));
        return out;
    }

    /* ---------- writes (Phase 2) — broker enforces permissions ---------- */

    public void deleteGroup(String clusterId, String groupId)
            throws ExecutionException, InterruptedException {
        AdminClient admin = clients.admin(clusterId);
        admin.deleteConsumerGroups(List.of(groupId)).all().get();
    }

    /**
     * Resets the committed offset of {@code (topic, partition)} for {@code groupId}.
     * For EARLIEST/LATEST the target offset is resolved via {@code listOffsets} to
     * ensure we commit a real boundary the broker recognises.
     */
    public long resetOffsets(String clusterId, String groupId, String topic, int partition,
                             ResetTarget target, long explicitOffset)
            throws ExecutionException, InterruptedException {
        AdminClient admin = clients.admin(clusterId);
        TopicPartition tp = new TopicPartition(topic, partition);
        long resolvedOffset;
        switch (target) {
            case EARLIEST -> {
                ListOffsetsResult res = admin.listOffsets(Map.of(tp, OffsetSpec.earliest()));
                resolvedOffset = res.partitionResult(tp).get().offset();
            }
            case LATEST -> {
                ListOffsetsResult res = admin.listOffsets(Map.of(tp, OffsetSpec.latest()));
                resolvedOffset = res.partitionResult(tp).get().offset();
            }
            case OFFSET -> {
                if (explicitOffset < 0) {
                    throw new IllegalArgumentException("Explicit offset must be >= 0");
                }
                resolvedOffset = explicitOffset;
            }
            default -> throw new IllegalStateException("Unhandled target: " + target);
        }
        Map<TopicPartition, OffsetAndMetadata> commits = new LinkedHashMap<>();
        commits.put(tp, new OffsetAndMetadata(resolvedOffset));
        admin.alterConsumerGroupOffsets(groupId, commits).all().get();
        return resolvedOffset;
    }
}
