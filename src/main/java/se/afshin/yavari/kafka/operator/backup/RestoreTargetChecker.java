package se.afshin.yavari.kafka.operator.backup;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import se.afshin.yavari.kafka.operator.crd.RestoreTargetPolicy;
import se.afshin.yavari.kafka.operator.topic.AdminClientTlsLoader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Restore pre-flight guard. Before a restore Job is created, verifies the resolved literal
 * target topics against the CR's {@link RestoreTargetPolicy} via a short-lived AdminClient
 * connected direct to the brokers.
 *
 * <p>Only literal topic names are checked — glob patterns and topics discovered from backup
 * metadata cannot be enumerated here. This is a guardrail, not a guarantee (a topic can fill
 * between the check and the Job starting).
 */
@ApplicationScoped
public class RestoreTargetChecker {

    @Inject AdminClientTlsLoader tlsLoader;

    /** @return null when the policy is satisfied, otherwise a violation message. */
    public String check(RestoreTargetPolicy policy, ResolvedBackupEndpoint endpoint,
                        String namespace, List<String> literalTopics) {
        if (policy == RestoreTargetPolicy.ALLOW_NON_EMPTY
                || literalTopics == null || literalTopics.isEmpty()) {
            return null;
        }
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.bootstrap());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        if (endpoint.hasTls()) {
            props.putAll(tlsLoader.loadAsAdminClientSslProps(namespace, endpoint.tlsSecretRef()));
        }
        try (Admin admin = Admin.create(props)) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> present = literalTopics.stream().filter(existing::contains).toList();
            if (policy == RestoreTargetPolicy.REQUIRE_ABSENT) {
                return present.isEmpty() ? null
                        : "targetPolicy=REQUIRE_ABSENT but target topics already exist: " + present;
            }
            // REQUIRE_EMPTY
            List<String> nonEmpty = new ArrayList<>();
            for (String topic : present) {
                if (!isEmpty(admin, topic)) {
                    nonEmpty.add(topic);
                }
            }
            return nonEmpty.isEmpty() ? null
                    : "targetPolicy=REQUIRE_EMPTY but target topics already hold data: " + nonEmpty;
        } catch (Exception e) {
            throw new RuntimeException("restore pre-flight topic check failed: " + e.getMessage(), e);
        }
    }

    private boolean isEmpty(Admin admin, String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic))
                .allTopicNames().get().get(topic);
        Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        description.partitions().forEach(p -> {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            earliest.put(tp, OffsetSpec.earliest());
            latest.put(tp, OffsetSpec.latest());
        });
        var earliestOffsets = admin.listOffsets(earliest).all().get();
        var latestOffsets = admin.listOffsets(latest).all().get();
        for (TopicPartition tp : earliestOffsets.keySet()) {
            if (earliestOffsets.get(tp).offset() != latestOffsets.get(tp).offset()) {
                return false;
            }
        }
        return true;
    }
}
