package se.afshin.yavari.kafka.operator.upgrade;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AnyNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ClusterEntry;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the /operator/upgrade-phase endpoint after the proxy CRD merge: the resource
 * now iterates KafkaClusters and checks each one's local Deployment for mid-roll state.
 * "Target" means the local cluster id appears in spec.clusters[] (multi-cluster) or the
 * topology is single-cluster.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class UpgradePhaseResourceTest {

    private static final String NS = "kafka";
    private static final String LOCAL_ID = "A";

    private KubernetesClient client;
    private UpgradePhaseResource resource;

    // PodSet chain
    private MixedOperation podSetOp;
    private AnyNamespaceOperation podSetList;

    // Cluster chain (where the merged proxy spec lives)
    private MixedOperation clusterOp;
    private AnyNamespaceOperation clusterList;

    // Deployment chain
    private AppsAPIGroupDSL appsApi;
    private MixedOperation depOp;
    private NonNamespaceOperation nsDepOp;
    private RollableScalableResource namedDep;

    @BeforeEach
    void setup() throws Exception {
        client = mock(KubernetesClient.class);

        KubernetesResourceList emptyPodSets = resourceList(List.of());
        KubernetesResourceList emptyClusters = resourceList(List.of());

        podSetOp = mock(MixedOperation.class);
        podSetList = mock(AnyNamespaceOperation.class);
        when(client.resources(KafkaPodSet.class)).thenReturn(podSetOp);
        when(podSetOp.inAnyNamespace()).thenReturn(podSetList);
        when(podSetList.list()).thenReturn(emptyPodSets);

        clusterOp = mock(MixedOperation.class);
        clusterList = mock(AnyNamespaceOperation.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterOp);
        when(clusterOp.inAnyNamespace()).thenReturn(clusterList);
        when(clusterList.list()).thenReturn(emptyClusters);

        appsApi = mock(AppsAPIGroupDSL.class);
        depOp = mock(MixedOperation.class);
        nsDepOp = mock(NonNamespaceOperation.class);
        namedDep = mock(RollableScalableResource.class);
        when(client.apps()).thenReturn(appsApi);
        when(appsApi.deployments()).thenReturn(depOp);
        when(depOp.inNamespace(anyString())).thenReturn(nsDepOp);
        when(nsDepOp.withName(anyString())).thenReturn(namedDep);

        resource = new UpgradePhaseResource();
        var clientField = resource.getClass().getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(resource, client);
        var idField = resource.getClass().getDeclaredField("localClusterId");
        idField.setAccessible(true);
        idField.set(resource, LOCAL_ID);
        var trackerField = resource.getClass().getDeclaredField("proxyRollTracker");
        trackerField.setAccessible(true);
        trackerField.set(resource, new se.afshin.yavari.kafka.operator.proxy.ProxyRollTracker());
    }

    @Test
    void noClusters_returnsIdle() {
        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
        assertThat(json).contains("\"clusterId\":\"A\"");
    }

    @Test
    void targetClusterProxyReady_returnsIdle() {
        givenClusters(clusterWithProxy(List.of("A", "B")));
        when(namedDep.get()).thenReturn(deployment(1L, 1L, 1, 1, 1));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
    }

    @Test
    void targetClusterProxyMidRoll_returnsRolling() {
        givenClusters(clusterWithProxy(List.of("A", "B")));
        // observedGeneration hasn't caught up — Kubernetes hasn't seen the new spec yet.
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 1, 1));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    @Test
    void targetClusterProxyMidRoll_unavailableReplicas_returnsRolling() {
        givenClusters(clusterWithProxy(List.of("A", "B")));
        when(namedDep.get()).thenReturn(deployment(1L, 1L, 1, 1, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    @Test
    void localNotInClustersList_doesNotFlipPhase() {
        // Multi-cluster topology {B} — local cluster A isn't in spec.clusters, so even a
        // stale deployment must NOT contribute to ROLLING.
        givenClusters(clusterWithProxy(List.of("B")));
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 0, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
    }

    @Test
    void singleClusterProxy_alwaysCountedAsTarget() {
        // Single-cluster topology: A is the only cluster, so a mid-roll surfaces as ROLLING.
        givenClusters(clusterWithProxy(List.of("A")));
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 0, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    // --- helpers ---

    private void givenClusters(KafkaCluster... clusters) {
        KubernetesResourceList list = resourceList(List.of(clusters));
        when(clusterList.list()).thenReturn(list);
    }

    private <T extends io.fabric8.kubernetes.api.model.HasMetadata> KubernetesResourceList<T> resourceList(List<T> items) {
        KubernetesResourceList<T> list = mock(KubernetesResourceList.class);
        when(list.getItems()).thenReturn(items);
        return list;
    }

    private KafkaCluster clusterWithProxy(List<String> clusterIds) {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-proxy");
        meta.setNamespace(NS);
        c.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        spec.setClusters(clusterIds.stream().map(id -> {
            ClusterEntry e = new ClusterEntry();
            e.setId(id);
            return e;
        }).toList());
        spec.setProxy(new KafkaClusterProxySpec());
        c.setSpec(spec);
        return c;
    }

    private Deployment deployment(long generation, long observedGeneration,
                                  int desired, int updated, int available) {
        Deployment d = new DeploymentBuilder()
                .withNewMetadata()
                    .withName("kafka-proxy")
                    .withNamespace(NS)
                    .withGeneration(generation)
                .endMetadata()
                .build();
        d.setSpec(new DeploymentSpecBuilder().withReplicas(desired).build());
        DeploymentStatus s = new DeploymentStatus();
        s.setObservedGeneration(observedGeneration);
        s.setUpdatedReplicas(updated);
        s.setAvailableReplicas(available);
        d.setStatus(s);
        return d;
    }
}
