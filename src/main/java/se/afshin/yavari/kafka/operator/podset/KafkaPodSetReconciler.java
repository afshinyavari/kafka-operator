package se.afshin.yavari.kafka.operator.podset;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import java.time.Duration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSetStatus;
import se.afshin.yavari.kafka.operator.crd.NodeRole;
import se.afshin.yavari.kafka.operator.crd.PodEntry;
import se.afshin.yavari.kafka.operator.crd.PodStatus;
import se.afshin.yavari.kafka.operator.crd.StorageSpec;
import se.afshin.yavari.kafka.operator.metrics.OperatorMetrics;
import se.afshin.yavari.kafka.operator.rolling.IsrChecker;
import se.afshin.yavari.kafka.operator.rolling.RollTracker;
import se.afshin.yavari.kafka.operator.rolling.RollingUpdateController;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;
import java.util.stream.Collectors;

@ControllerConfiguration
@ApplicationScoped
public class KafkaPodSetReconciler implements Reconciler<KafkaPodSet>, Cleaner<KafkaPodSet>,
        EventSourceInitializer<KafkaPodSet> {

    private static final Logger LOG = Logger.getLogger(KafkaPodSetReconciler.class);

    @Inject KubernetesClient client;
    @Inject RollingUpdateController rollingController;
    @Inject IsrChecker isrChecker;
    @Inject PodSpecHasher podSpecHasher;
    @Inject PvcFactory pvcFactory;
    @Inject OperatorMetrics metrics;
    @Inject se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator rollCoordinator;
    @Inject RollTracker rollTracker;
    @ConfigProperty(name = "kafka.cluster.id") String localClusterId;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<KafkaPodSet> context) {
        var podEventSource = new InformerEventSource<>(
            InformerConfiguration.from(Pod.class, context)
                .withSecondaryToPrimaryMapper(pod -> {
                    Map<String, String> labels = pod.getMetadata().getLabels();
                    if (labels == null) return Set.of();
                    String poolName = labels.get(KafkaPodSet.NODE_POOL_LABEL);
                    if (poolName == null) return Set.of();
                    return Set.of(new ResourceID(poolName + "-podset",
                            pod.getMetadata().getNamespace()));
                })
                .build(),
            context);
        return EventSourceInitializer.nameEventSources(podEventSource);
    }

    @Override
    public UpdateControl<KafkaPodSet> reconcile(KafkaPodSet podSet, Context<KafkaPodSet> context) {
        try (var ignored = se.afshin.yavari.kafka.operator.infra.ReconcileContext.scope(podSet)) {
        return reconcileInner(podSet, context);
        }
    }

    private UpdateControl<KafkaPodSet> reconcileInner(KafkaPodSet podSet, Context<KafkaPodSet> context) {
        String name = podSet.getMetadata().getName();
        String namespace = podSet.getMetadata().getNamespace();
        LOG.infof("Reconciling KafkaPodSet %s/%s", namespace, name);

        KafkaPodSetStatus status = podSet.getStatus() != null ? podSet.getStatus() : new KafkaPodSetStatus();
        status.setReplicas(podSet.getSpec().getPods().size());

        Map<String, String> selector = podSet.getSpec().getSelector().getMatchLabels();
        List<Pod> actualPods = client.pods().inNamespace(namespace).withLabels(selector).list().getItems();
        Map<String, Pod> actualByName = actualPods.stream()
                .collect(Collectors.toMap(p -> p.getMetadata().getName(), p -> p));

        List<PodEntry> desired = podSet.getSpec().getPods();
        Set<String> desiredNames = desired.stream()
                .map(e -> e.getMetadata().getName())
                .collect(Collectors.toSet());

        List<NodeRole> poolRoles = resolveRoles(podSet, namespace);
        boolean brokerPool = poolRoles.contains(NodeRole.BROKER);
        boolean controllerPool = poolRoles.contains(NodeRole.CONTROLLER);
        StorageSpec storage = resolveStorage(podSet, namespace);

        // Scale down: ISR/quorum safety check before deleting each excess pod
        boolean pendingScaleDown = false;
        for (Pod actual : actualPods) {
            if (!desiredNames.contains(actual.getMetadata().getName())) {
                int nodeId = resolveNodeId(actual);
                boolean safe = true;

                if (brokerPool && nodeId >= 0) {
                    safe = isrChecker.isBrokerSafeToRestart(bootstrapAddress(podSet, namespace), nodeId);
                }
                if (controllerPool && nodeId >= 0 && safe) {
                    safe = isrChecker.isControllerSafeToRestart(controllerBootstrapAddress(podSet, namespace), nodeId);
                }

                String poolName = podSet.getMetadata().getLabels().getOrDefault(KafkaPodSet.NODE_POOL_LABEL, "unknown");
                metrics.recordScaleDown(namespace, poolName, safe);
                if (safe) {
                    LOG.infof("Scale-down: deleting pod %s (node %d)", actual.getMetadata().getName(), nodeId);
                    client.pods().inNamespace(namespace).withName(actual.getMetadata().getName()).delete();
                    // PVC is intentionally NOT deleted to preserve data
                } else {
                    LOG.warnf("Scale-down: pod %s (node %d) not yet safe to remove — will retry",
                            actual.getMetadata().getName(), nodeId);
                    pendingScaleDown = true;
                }
            }
        }

        // Cross-cluster roll order gate: applies to ALL pools (brokers and controllers).
        // Previously controllers-only — broker rolls now follow the same clusterRollOrder
        // sequencing so simultaneous spec changes (cert rotation, image bumps) can't roll
        // every replica across clusters at once and breach min.insync.replicas.
        // Skip the gate when no roll is actually pending — we don't want to block a healthy
        // reconcile pass that's just refreshing status.
        boolean rollIsRequired = anyDesiredHashMismatch(desired, actualByName);
        if (rollIsRequired) {
            KafkaCluster parentCluster = lookupParentCluster(podSet, namespace);
            if (parentCluster != null
                    && !rollCoordinator.isMyTurnToRoll(parentCluster.getSpec(), localClusterId)) {
                LOG.infof("KafkaPodSet %s/%s: cross-cluster roll gate — not my turn yet, deferring 15s",
                        namespace, name);
                podSet.setStatus(status);
                return UpdateControl.patchStatus(podSet).rescheduleAfter(Duration.ofSeconds(15));
            }
            // Claim the ROLLING signal BEFORE calling into rollPod() so a successor cluster
            // polling /operator/upgrade-phase sees ROLLING during the multi-minute roll
            // window. Without this the etcd-backed status check at UpgradePhaseResource
            // never observes a successful roll (status.currentRollingPod is cleared back
            // to "" before patchStatus runs).
            rollTracker.markRolling("podset", namespace, name);
        }

        String currentRolling = status.getCurrentRollingPod();
        List<PodStatus> podStatuses = new ArrayList<>();

        for (PodEntry entry : desired) {
            String podName = entry.getMetadata().getName();
            String desiredHash = podSpecHasher.hash(entry.getSpec());
            Pod actual = actualByName.get(podName);

            PodStatus ps = new PodStatus();
            ps.setName(podName);
            ps.setSpecHash(desiredHash);

            if (actual == null) {
                // Scale up: create pod + PVC
                LOG.infof("Scale-up: creating pod %s", podName);
                pvcFactory.ensure(entry, namespace, podSet, storage);
                annotateWithHash(entry, desiredHash);
                client.pods().inNamespace(namespace).resource(buildPod(entry)).create();
                ps.setCurrentSpecHash(desiredHash);
                ps.setReady(false);
            } else {
                String currentHash = actual.getMetadata().getAnnotations() != null
                        ? actual.getMetadata().getAnnotations().getOrDefault(KafkaPodSet.SPEC_HASH_ANNOTATION, "")
                        : "";
                ps.setCurrentSpecHash(currentHash);
                ps.setReady(isPodReady(actual));

                if (!desiredHash.equals(currentHash)) {
                    if (!currentRolling.isEmpty() && !currentRolling.equals(podName)) {
                        LOG.infof("Pod %s needs update but %s is currently rolling — skipping this cycle", podName, currentRolling);
                    } else {
                        LOG.infof("Rolling update: pod %s (hash %s → %s)", podName, currentHash, desiredHash);
                        status.setCurrentRollingPod(podName);
                        annotateWithHash(entry, desiredHash);
                        pvcFactory.ensure(entry, namespace, podSet, storage);

                        String bootstrapAddr = bootstrapAddress(podSet, namespace);
                        int nodeId = resolveNodeId(actual);

                        try {
                            rollingController.rollPod(entry, poolRoles, bootstrapAddr, nodeId, client, namespace);
                            ps.setCurrentSpecHash(desiredHash);
                            ps.setReady(true);
                            status.setCurrentRollingPod("");
                        } catch (Exception e) {
                            LOG.errorf("Rolling update of pod %s failed: %s", podName, e.getMessage());
                            status.setCurrentRollingPod(podName);
                        }
                    }
                }
            }

            Pod refreshed = client.pods().inNamespace(namespace).withName(podName).get();
            if (refreshed != null) ps.setReady(isPodReady(refreshed));

            podStatuses.add(ps);
        }

        status.setPods(podStatuses);
        int readyCount = (int) podStatuses.stream().filter(PodStatus::isReady).count();
        status.setReadyReplicas(readyCount);

        podSet.setStatus(status);
        boolean needsRecheck = readyCount < status.getReplicas() || !status.getCurrentRollingPod().isEmpty() || pendingScaleDown;
        if (!needsRecheck) {
            // Steady state: clear the ROLLING signal so a successor cluster can claim its turn.
            rollTracker.markComplete("podset", namespace, name);
        }
        if (needsRecheck) {
            return UpdateControl.patchStatus(podSet).rescheduleAfter(Duration.ofSeconds(15));
        }
        return UpdateControl.patchStatus(podSet);
    }

    private boolean anyDesiredHashMismatch(List<PodEntry> desired, Map<String, Pod> actualByName) {
        for (PodEntry entry : desired) {
            Pod actual = actualByName.get(entry.getMetadata().getName());
            if (actual == null) continue; // scale-up, not a roll
            String desiredHash = podSpecHasher.hash(entry.getSpec());
            String currentHash = actual.getMetadata().getAnnotations() != null
                    ? actual.getMetadata().getAnnotations().getOrDefault(KafkaPodSet.SPEC_HASH_ANNOTATION, "")
                    : "";
            if (!desiredHash.equals(currentHash)) return true;
        }
        return false;
    }

    @Override
    public DeleteControl cleanup(KafkaPodSet podSet, Context<KafkaPodSet> context) {
        String namespace = podSet.getMetadata().getNamespace();
        Map<String, String> selector = podSet.getSpec().getSelector().getMatchLabels();
        LOG.infof("Cleaning up KafkaPodSet %s — deleting pods", podSet.getMetadata().getName());
        client.pods().inNamespace(namespace).withLabels(selector).list().getItems()
              .forEach(pod -> client.pods().inNamespace(namespace)
                                   .withName(pod.getMetadata().getName()).delete());
        return DeleteControl.defaultDelete();
    }

    private Pod buildPod(PodEntry entry) {
        Pod pod = new Pod();
        pod.setMetadata(entry.getMetadata());
        pod.setSpec(entry.getSpec());
        return pod;
    }

    private void annotateWithHash(PodEntry entry, String hash) {
        Map<String, String> annotations = entry.getMetadata().getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>();
            entry.getMetadata().setAnnotations(annotations);
        }
        annotations.put(KafkaPodSet.SPEC_HASH_ANNOTATION, hash);
    }

    private boolean isPodReady(Pod pod) {
        if (pod == null || pod.getStatus() == null || pod.getStatus().getConditions() == null) return false;
        return pod.getStatus().getConditions().stream()
                .filter(c -> "Ready".equals(c.getType()))
                .anyMatch(c -> "True".equals(c.getStatus()));
    }

    private KafkaCluster lookupParentCluster(KafkaPodSet podSet, String namespace) {
        String clusterName = podSet.getMetadata().getLabels().get(KafkaPodSet.CLUSTER_LABEL);
        if (clusterName == null) return null;
        return client.resources(KafkaCluster.class).inNamespace(namespace).withName(clusterName).get();
    }

    private List<NodeRole> resolveRoles(KafkaPodSet podSet, String namespace) {
        String poolName = podSet.getMetadata().getLabels().get(KafkaPodSet.NODE_POOL_LABEL);
        if (poolName == null) return List.of(NodeRole.BROKER);
        KafkaNodePool pool = client.resources(KafkaNodePool.class).inNamespace(namespace).withName(poolName).get();
        return pool != null ? pool.getSpec().getRoles() : List.of(NodeRole.BROKER);
    }

    private StorageSpec resolveStorage(KafkaPodSet podSet, String namespace) {
        String poolName = podSet.getMetadata().getLabels().get(KafkaPodSet.NODE_POOL_LABEL);
        if (poolName == null) return new StorageSpec();
        KafkaNodePool pool = client.resources(KafkaNodePool.class).inNamespace(namespace).withName(poolName).get();
        return pool != null ? pool.getSpec().getStorage() : new StorageSpec();
    }

    private String bootstrapAddress(KafkaPodSet podSet, String namespace) {
        String poolName = podSet.getMetadata().getLabels().getOrDefault(KafkaPodSet.NODE_POOL_LABEL, "kafka");
        return poolName + "-headless." + namespace + ".svc.cluster.local:9092";
    }

    private String controllerBootstrapAddress(KafkaPodSet podSet, String namespace) {
        String poolName = podSet.getMetadata().getLabels().getOrDefault(KafkaPodSet.NODE_POOL_LABEL, "kafka");
        return poolName + "-headless." + namespace + ".svc.cluster.local:9093";
    }

    private int resolveNodeId(Pod pod) {
        String nodeIdStr = pod.getMetadata().getLabels().get(KafkaPodSet.NODE_ID_LABEL);
        try {
            return nodeIdStr != null ? Integer.parseInt(nodeIdStr) : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
