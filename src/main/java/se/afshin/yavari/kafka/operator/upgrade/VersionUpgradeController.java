package se.afshin.yavari.kafka.operator.upgrade;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.config.KRaftConfigGenerator;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class VersionUpgradeController {

    private static final Logger LOG = Logger.getLogger(VersionUpgradeController.class);
    private static final String METADATA_VERSION_FEATURE = "metadata.version";
    private static final int TIMEOUT_SECONDS = 30;

    public void reconcile(KafkaCluster cluster, List<Pod> allPods,
                          String namespace, KafkaClusterStatus status) {
        String specVersion = cluster.getSpec().getKafkaVersion();
        Integer targetMetadataVersion = cluster.getSpec().getTargetMetadataVersion();

        if (allPods.isEmpty()) {
            status.setUpgradePhase("IDLE");
            return;
        }

        List<String> podVersions = allPods.stream()
                .map(p -> p.getMetadata().getAnnotations())
                .filter(a -> a != null && a.containsKey(KafkaPodSet.KAFKA_VERSION_ANNOTATION))
                .map(a -> a.get(KafkaPodSet.KAFKA_VERSION_ANNOTATION))
                .toList();

        if (podVersions.isEmpty()) {
            status.setUpgradePhase("IDLE");
            return;
        }

        Optional<String> minVersion = podVersions.stream()
                .min((a, b) -> compareVersions(a, b));
        minVersion.ifPresent(status::setCurrentKafkaVersion);

        boolean allOnTarget = podVersions.stream().allMatch(v -> v.equals(specVersion));

        if (!allOnTarget) {
            status.setUpgradePhase("ROLLING");
            return;
        }

        status.setCurrentKafkaVersion(specVersion);

        if (targetMetadataVersion == null) {
            status.setUpgradePhase("IDLE");
            return;
        }

        Optional<String> brokerBootstrap = findBrokerBootstrap(allPods, namespace);
        if (brokerBootstrap.isEmpty()) {
            LOG.infof("KafkaCluster %s/%s: no ready broker found for metadata.version check — will retry",
                    cluster.getMetadata().getNamespace(), cluster.getMetadata().getName());
            status.setUpgradePhase("METADATA_PENDING");
            return;
        }

        reconcileMetadataVersion(brokerBootstrap.get(), targetMetadataVersion, status,
                cluster.getMetadata().getNamespace(), cluster.getMetadata().getName());
    }

    private void reconcileMetadataVersion(String bootstrapAddress, int targetMetadataVersion,
                                          KafkaClusterStatus status, String namespace, String name) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_SECONDS * 1000));

        try (AdminClient admin = AdminClient.create(props)) {
            Map<String, FinalizedVersionRange> features = admin.describeFeatures()
                    .featureMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .finalizedFeatures();

            FinalizedVersionRange currentRange = features.get(METADATA_VERSION_FEATURE);
            int currentVersion = currentRange != null ? currentRange.maxVersionLevel() : 0;
            status.setCurrentMetadataVersion(currentVersion);

            if (currentVersion >= targetMetadataVersion) {
                status.setUpgradePhase("COMPLETE");
                return;
            }

            LOG.infof("KafkaCluster %s/%s: upgrading metadata.version %d → %d",
                    namespace, name, currentVersion, targetMetadataVersion);
            status.setUpgradePhase("METADATA_PENDING");

            admin.updateFeatures(
                    Map.of(METADATA_VERSION_FEATURE,
                            new FeatureUpdate((short) targetMetadataVersion, FeatureUpdate.UpgradeType.UPGRADE)),
                    new org.apache.kafka.clients.admin.UpdateFeaturesOptions()
            ).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            status.setCurrentMetadataVersion(targetMetadataVersion);
            status.setUpgradePhase("COMPLETE");
            LOG.infof("KafkaCluster %s/%s: metadata.version upgraded to %d", namespace, name, targetMetadataVersion);

        } catch (Exception e) {
            LOG.warnf("KafkaCluster %s/%s: failed to upgrade metadata.version — will retry: %s",
                    namespace, name, e.getMessage());
            status.setUpgradePhase("METADATA_PENDING");
        }
    }

    private Optional<String> findBrokerBootstrap(List<Pod> allPods, String namespace) {
        return allPods.stream()
                .filter(this::isPodReady)
                .filter(p -> isBrokerPod(p))
                .findFirst()
                .map(p -> {
                    String poolName = p.getMetadata().getLabels()
                            .getOrDefault(KafkaPodSet.NODE_POOL_LABEL, "");
                    return poolName + "-headless." + namespace + ".svc.cluster.local:9092";
                });
    }

    private boolean isBrokerPod(Pod pod) {
        String nodeIdStr = pod.getMetadata().getLabels()
                .getOrDefault(KafkaPodSet.NODE_ID_LABEL, "-1");
        try {
            return Integer.parseInt(nodeIdStr) < KRaftConfigGenerator.CONTROLLER_BASE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isPodReady(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getConditions() == null) {
            return false;
        }
        return pod.getStatus().getConditions().stream()
                .filter(c -> "Ready".equals(c.getType()))
                .anyMatch(c -> "True".equals(c.getStatus()));
    }

    private int compareVersions(String a, String b) {
        int[] partsA = parseVersion(a);
        int[] partsB = parseVersion(b);
        int cmp = Integer.compare(partsA[0], partsB[0]);
        return cmp != 0 ? cmp : Integer.compare(partsA[1], partsB[1]);
    }

    private int[] parseVersion(String version) {
        try {
            String[] parts = version.split("\\.", 2);
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return new int[]{major, minor};
        } catch (NumberFormatException e) {
            return new int[]{0, 0};
        }
    }
}
