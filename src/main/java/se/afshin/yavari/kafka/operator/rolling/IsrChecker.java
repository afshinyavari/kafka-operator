package se.afshin.yavari.kafka.operator.rolling;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.metrics.OperatorMetrics;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class IsrChecker {

    private static final Logger LOG = Logger.getLogger(IsrChecker.class);
    private static final int TIMEOUT_SECONDS = 30;

    @Inject OperatorMetrics metrics;

    /**
     * Returns true if it is safe to restart the given broker node.
     * A node is safe to restart when it is not the sole ISR member for any partition.
     * If Kafka is unreachable (cluster bootstrapping), logs a warning and returns true
     * so the operator does not get stuck on a fresh install.
     *
     * @param bootstrapAddress "host:port" for any Kafka broker in the cluster
     * @param nodeId           the Kafka node.id of the broker about to be restarted
     */
    public boolean isBrokerSafeToRestart(String bootstrapAddress, int nodeId) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> topicNames = admin.listTopics().names().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (topicNames.isEmpty()) {
                return true;
            }

            Map<String, TopicDescription> descriptions =
                    admin.describeTopics(topicNames).allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (TopicDescription td : descriptions.values()) {
                for (TopicPartitionInfo tpi : td.partitions()) {
                    boolean inIsr = tpi.isr().stream().anyMatch(n -> n.id() == nodeId);
                    if (inIsr && tpi.isr().size() == 1) {
                        LOG.warnf("Node %d is the sole ISR member for %s-%d — not safe to restart yet",
                                nodeId, td.name(), tpi.partition());
                        metrics.recordIsrCheck("broker", false);
                        return false;
                    }
                }
            }
            metrics.recordIsrCheck("broker", true);
            return true;

        } catch (Exception e) {
            LOG.warnf("Cannot reach Kafka at %s to check ISR (node %d) — treating as safe: %s",
                    bootstrapAddress, nodeId, e.getMessage());
            metrics.recordIsrCheck("broker", true);
            return true;
        }
    }

    /**
     * Returns true if the KRaft controller quorum is healthy enough to safely restart
     * the controller at the given node ID (i.e. there is a leader and other voters are caught up).
     *
     * @param bootstrapControllerAddress "host:9093" of any controller in the quorum
     * @param nodeId                     node ID of the controller about to be restarted
     */
    public boolean isControllerSafeToRestart(String bootstrapControllerAddress, int nodeId) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapControllerAddress);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));

        try (AdminClient admin = AdminClient.create(props)) {
            QuorumInfo quorum = admin.describeMetadataQuorum()
                    .quorumInfo().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Collection<QuorumInfo.ReplicaState> voters = quorum.voters();
            if (voters.size() < 2) {
                LOG.warnf("Controller quorum has only %d voter(s) — not safe to restart node %d",
                        voters.size(), nodeId);
                return false;
            }

            // Ensure other voters are caught up (lag == 0)
            long laggedPeers = voters.stream()
                    .filter(v -> v.replicaId() != nodeId)
                    .filter(v -> v.lastCaughtUpTimestamp().isEmpty() || v.logEndOffset() < quorum.highWatermark())
                    .count();

            if (laggedPeers > 0) {
                LOG.warnf("Controller quorum has %d lagged peer(s) — not safe to restart node %d yet",
                        laggedPeers, nodeId);
                metrics.recordIsrCheck("controller", false);
                return false;
            }
            metrics.recordIsrCheck("controller", true);
            return true;

        } catch (Exception e) {
            LOG.warnf("Cannot reach controller at %s to check quorum (node %d) — treating as safe: %s",
                    bootstrapControllerAddress, nodeId, e.getMessage());
            metrics.recordIsrCheck("controller", true);
            return true;
        }
    }
}
