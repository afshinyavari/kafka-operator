package se.afshin.yavari.kafka.ui.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;
import se.afshin.yavari.kafka.ui.kafka.KafkaClientProvider;
import se.afshin.yavari.kafka.ui.rbac.UserRbac;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class TopicService {

    @Inject KafkaClientProvider clients;

    public record TopicSummary(String name, int partitions, int replication, boolean internal) {}

    public record TopicDetail(
            String name,
            List<PartitionInfo> partitions,
            List<ConfigKv> configs,
            boolean internal) {}

    public record PartitionInfo(int id, int leader, List<Integer> replicas, List<Integer> isr) {}

    public record ConfigKv(String key, String value, boolean isDefault, boolean sensitive) {}

    public List<TopicSummary> list(String clusterId, UserRbac rbac) throws ExecutionException, InterruptedException {
        AdminClient admin = clients.admin(clusterId);
        ListTopicsOptions opts = new ListTopicsOptions().listInternal(false);
        Collection<TopicListing> all = admin.listTopics(opts).listings().get();
        List<String> visibleNames = all.stream()
                .map(TopicListing::name)
                .filter(rbac::canReadTopic)
                .toList();
        if (visibleNames.isEmpty()) return List.of();

        Map<String, TopicDescription> descs = admin.describeTopics(visibleNames).allTopicNames().get();
        List<TopicSummary> out = new ArrayList<>(descs.size());
        for (TopicDescription d : descs.values()) {
            int replication = d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
            out.add(new TopicSummary(d.name(), d.partitions().size(), replication, d.isInternal()));
        }
        out.sort(Comparator.comparing(TopicSummary::name));
        return out;
    }

    public TopicDetail describe(String clusterId, String name, UserRbac rbac)
            throws ExecutionException, InterruptedException {
        if (!rbac.canReadTopic(name)) {
            throw new IllegalAccessError("Topic " + name + " not visible to user");
        }
        AdminClient admin = clients.admin(clusterId);
        TopicDescription d = admin.describeTopics(List.of(name)).allTopicNames().get().get(name);

        // describeConfigs needs DESCRIBE_CONFIGS — a separate operation from
        // FETCH/READ/DESCRIBE. Most regular users won't have it. Treat an
        // authz failure as "configs not visible" and render the rest.
        List<ConfigKv> configs;
        try {
            ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, name);
            Config cfg = admin.describeConfigs(List.of(res)).all().get().get(res);
            configs = new ArrayList<>();
            for (ConfigEntry e : cfg.entries()) {
                configs.add(new ConfigKv(e.name(), e.value(), e.isDefault(), e.isSensitive()));
            }
            configs.sort(Comparator.comparing(ConfigKv::key));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.AuthorizationException) {
                configs = List.of();
            } else throw e;
        }

        List<PartitionInfo> parts = new ArrayList<>();
        d.partitions().forEach(p -> parts.add(new PartitionInfo(
                p.partition(),
                p.leader() == null ? -1 : p.leader().id(),
                p.replicas().stream().map(n -> n.id()).toList(),
                p.isr().stream().map(n -> n.id()).toList())));
        parts.sort(Comparator.comparingInt(PartitionInfo::id));

        return new TopicDetail(d.name(), parts, configs, d.isInternal());
    }
}
