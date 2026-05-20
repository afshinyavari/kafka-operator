package se.afshin.yavari.kafka.operator.topic;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Owns AdminClient interactions and pure diff logic for KafkaTopic reconciliation.
 * Kept separate from the reconciler so the reconciler stays thin and the diff
 * logic is unit-testable without a live broker.
 */
@ApplicationScoped
public class KafkaTopicService {

    private static final Logger LOG = Logger.getLogger(KafkaTopicService.class);

    /** Per-AdminClient API call timeout. Kept short (5s) so that a stuck call —
     *  e.g. against a broker whose listener is SSL while we're plaintext — fails
     *  fast instead of leaking buffers and OOM-killing the operator pod. */
    int timeoutSeconds = 5;

    /** Snapshot of a topic's observable state from Kafka. {@code null} = topic does not exist. */
    public record TopicState(String topicId, int partitions, short replicationFactor,
                             Map<String, String> dynamicConfig) {}

    public AdminClient newAdmin(String bootstrap) {
        return newAdmin(bootstrap, null);
    }

    /** @param sslProps when non-null/non-empty, AdminClient connects over SSL using PEM material from this map. */
    public AdminClient newAdmin(String bootstrap, Properties sslProps) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(timeoutSeconds * 1000));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(timeoutSeconds * 1000));
        if (sslProps != null && !sslProps.isEmpty()) {
            props.putAll(sslProps);
        }
        return AdminClient.create(props);
    }

    /** Describe the topic. Returns {@code null} when the topic does not exist. */
    public TopicState describe(AdminClient admin, String topicName)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        TopicDescription desc;
        try {
            desc = admin.describeTopics(List.of(topicName))
                    .allTopicNames().get(timeoutSeconds, TimeUnit.SECONDS).get(topicName);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) return null;
            throw e;
        }
        if (desc == null) return null;

        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config config = admin.describeConfigs(List.of(cr))
                .all().get(timeoutSeconds, TimeUnit.SECONDS).get(cr);

        Map<String, String> dyn = new HashMap<>();
        for (ConfigEntry e : config.entries()) {
            if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                dyn.put(e.name(), e.value());
            }
        }

        short rf = desc.partitions().isEmpty()
                ? 0
                : (short) desc.partitions().get(0).replicas().size();
        return new TopicState(
                desc.topicId() != null ? desc.topicId().toString() : null,
                desc.partitions().size(),
                rf,
                dyn);
    }

    public TopicState createTopic(AdminClient admin, String name, int partitions,
                                  short replicationFactor, Map<String, String> config)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        NewTopic nt = new NewTopic(name, partitions, replicationFactor).configs(config);
        try {
            admin.createTopics(List.of(nt)).all().get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw e;
            LOG.debugf("Topic %s already exists (raced with peer operator)", name);
        }
        // Eventual consistency: a describe immediately after createTopics can race the metadata
        // propagation and return null. Retry a few times (cheap, in-broker) before giving up.
        for (int attempt = 0; attempt < 5; attempt++) {
            TopicState state = describe(admin, name);
            if (state != null) return state;
            try { Thread.sleep(200L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }
        return null;
    }

    /**
     * Compute incremental config alterations needed to make {@code current} match {@code desired}.
     * Entries in {@code desired} take precedence; entries present in {@code current} but absent
     * from {@code desired} are reset to broker defaults (declarative semantics).
     */
    public List<AlterConfigOp> computeConfigDiff(Map<String, String> current, Map<String, String> desired) {
        Map<String, String> want = desired != null ? desired : Map.of();
        Map<String, String> have = current != null ? current : Map.of();

        List<AlterConfigOp> ops = new ArrayList<>();
        for (var e : want.entrySet()) {
            String haveVal = have.get(e.getKey());
            if (!e.getValue().equals(haveVal)) {
                ops.add(new AlterConfigOp(
                        new ConfigEntry(e.getKey(), e.getValue()),
                        AlterConfigOp.OpType.SET));
            }
        }
        Set<String> toUnset = new HashSet<>(have.keySet());
        toUnset.removeAll(want.keySet());
        for (String key : toUnset) {
            ops.add(new AlterConfigOp(new ConfigEntry(key, null), AlterConfigOp.OpType.DELETE));
        }
        return ops;
    }

    public void applyConfigDiff(AdminClient admin, String topicName, List<AlterConfigOp> ops)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        if (ops.isEmpty()) return;
        ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        admin.incrementalAlterConfigs(Map.of(cr, ops))
                .all().get(timeoutSeconds, TimeUnit.SECONDS);
    }

    public enum PartitionAction { NONE, EXPAND, REJECT_DECREASE }

    public PartitionAction computePartitionAction(int currentPartitions, int desiredPartitions) {
        if (desiredPartitions == currentPartitions) return PartitionAction.NONE;
        if (desiredPartitions > currentPartitions) return PartitionAction.EXPAND;
        return PartitionAction.REJECT_DECREASE;
    }

    public void increasePartitions(AdminClient admin, String topicName, int totalPartitions)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(totalPartitions)))
                .all().get(timeoutSeconds, TimeUnit.SECONDS);
    }

    public void deleteTopic(AdminClient admin, String topicName)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        try {
            admin.deleteTopics(List.of(topicName)).all().get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                LOG.debugf("Topic %s already gone — treating delete as success", topicName);
                return;
            }
            throw e;
        }
    }

    /** Test-only: stable list view of a diff, for assertions. */
    static List<AlterConfigOp> sorted(Collection<AlterConfigOp> ops) {
        List<AlterConfigOp> copy = new ArrayList<>(ops);
        copy.sort((a, b) -> a.configEntry().name().compareTo(b.configEntry().name()));
        return Collections.unmodifiableList(copy);
    }
}
