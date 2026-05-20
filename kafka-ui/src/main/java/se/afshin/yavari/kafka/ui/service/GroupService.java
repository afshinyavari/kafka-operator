package se.afshin.yavari.kafka.ui.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class GroupService {

    @Inject KafkaClientProvider clients;

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
}
