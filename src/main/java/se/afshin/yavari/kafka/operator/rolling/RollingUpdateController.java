package se.afshin.yavari.kafka.operator.rolling;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class RollingUpdateController {

    private static final Logger LOG = Logger.getLogger(RollingUpdateController.class);
    private static final int ISR_POLL_INTERVAL_SECONDS = 10;
    private static final int ISR_POLL_MAX_ATTEMPTS = 30;  // 5 minutes total
    private static final int POD_READY_TIMEOUT_MINUTES = 5;
    private static final int POD_GONE_TIMEOUT_MINUTES = 2;

    @Inject
    IsrChecker isrChecker;

    /**
     * Safely replaces one pod in a KafkaPodSet with the desired spec.
     *
     * Steps:
     *  1. Wait for ISR / quorum safety before touching this node
     *  2. Delete the existing pod (PVC is preserved)
     *  3. Wait for the old pod to disappear
     *  4. Create the new pod with the desired spec
     *  5. Wait for the new pod to become Ready
     *  6. Verify ISR recovery before returning
     *
     * @param desired          the desired PodEntry from KafkaPodSetSpec
     * @param roles            roles of this pool (determines ISR check type)
     * @param bootstrapAddress broker or controller bootstrap for ISR/quorum check
     * @param nodeId           Kafka node.id of this pod
     * @param client           fabric8 client for the local cluster
     * @param namespace        namespace where the pod lives
     */
    public void rollPod(PodEntry desired,
                        List<NodeRole> roles,
                        String bootstrapAddress,
                        int nodeId,
                        KubernetesClient client,
                        String namespace) {

        String podName = desired.getMetadata().getName();
        LOG.infof("Rolling pod %s (node.id=%d)", podName, nodeId);

        // Step 1: ISR / quorum safety check
        waitForSafe(bootstrapAddress, nodeId, roles);

        // Step 2: Delete existing pod
        client.pods().inNamespace(namespace).withName(podName).delete();
        LOG.infof("Deleted pod %s — waiting for it to disappear", podName);

        // Step 3: Wait for pod to be gone
        waitForPodGone(client, namespace, podName);

        // Step 4: Create new pod
        Pod newPod = buildPod(desired);
        client.pods().inNamespace(namespace).resource(newPod).create();
        LOG.infof("Created new pod %s — waiting for Ready", podName);

        // Step 5: Wait for Ready
        waitForPodReady(client, namespace, podName);

        // Step 6: Verify ISR recovery
        waitForSafe(bootstrapAddress, nodeId, roles);
        LOG.infof("Pod %s rolled successfully", podName);
    }

    private void waitForSafe(String bootstrapAddress, int nodeId, List<NodeRole> roles) {
        boolean isController = roles.contains(NodeRole.CONTROLLER);
        for (int attempt = 0; attempt < ISR_POLL_MAX_ATTEMPTS; attempt++) {
            boolean safe = isController
                    ? isrChecker.isControllerSafeToRestart(bootstrapAddress, nodeId)
                    : isrChecker.isBrokerSafeToRestart(bootstrapAddress, nodeId);
            if (safe) return;
            LOG.infof("Node %d not yet safe to restart (attempt %d/%d) — waiting %ds",
                    nodeId, attempt + 1, ISR_POLL_MAX_ATTEMPTS, ISR_POLL_INTERVAL_SECONDS);
            sleep(ISR_POLL_INTERVAL_SECONDS);
        }
        throw new RollingUpdateException(
                "Timed out waiting for node " + nodeId + " to be safe to restart after "
                + (ISR_POLL_MAX_ATTEMPTS * ISR_POLL_INTERVAL_SECONDS) + "s");
    }

    private void waitForPodGone(KubernetesClient client, String namespace, String podName) {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(POD_GONE_TIMEOUT_MINUTES);
        while (System.currentTimeMillis() < deadline) {
            Pod p = client.pods().inNamespace(namespace).withName(podName).get();
            if (p == null) return;
            sleep(2);
        }
        throw new RollingUpdateException("Timed out waiting for pod " + podName + " to disappear");
    }

    private void waitForPodReady(KubernetesClient client, String namespace, String podName) {
        CountDownLatch ready = new CountDownLatch(1);
        try (Watch watch = client.pods().inNamespace(namespace).withName(podName)
                .watch(new Watcher<>() {
                    @Override
                    public void eventReceived(Action action, Pod pod) {
                        if (isPodReady(pod)) ready.countDown();
                    }
                    @Override
                    public void onClose(WatcherException e) {
                        if (e != null) LOG.warnf("Watch closed with error: %s", e.getMessage());
                    }
                })) {
            if (!ready.await(POD_READY_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                throw new RollingUpdateException(
                        "Timed out waiting for pod " + podName + " to become Ready");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RollingUpdateException("Interrupted while waiting for pod " + podName);
        }
    }

    private boolean isPodReady(Pod pod) {
        if (pod == null || pod.getStatus() == null || pod.getStatus().getConditions() == null) {
            return false;
        }
        return pod.getStatus().getConditions().stream()
                .filter(c -> "Ready".equals(c.getType()))
                .map(PodCondition::getStatus)
                .anyMatch("True"::equals);
    }

    private Pod buildPod(PodEntry entry) {
        Pod pod = new Pod();
        pod.setMetadata(entry.getMetadata());
        pod.setSpec(entry.getSpec());
        return pod;
    }

    private void sleep(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
