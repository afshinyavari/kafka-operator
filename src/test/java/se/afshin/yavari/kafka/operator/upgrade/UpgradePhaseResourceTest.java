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
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaPodSet;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that the /operator/upgrade-phase endpoint flips to ROLLING while the local
 * KafkaProxy Deployment is mid-roll, but only when this cluster is a target of the proxy CR.
 * CrossClusterRollCoordinator on the successor cluster polls this endpoint to decide whether
 * to start its own proxy roll, so a false ROLLING (e.g. for a SKIPPED proxy) would deadlock
 * the upgrade sequence.
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

    // Proxy chain
    private MixedOperation proxyOp;
    private AnyNamespaceOperation proxyList;

    // Cluster chain (for fallback phase)
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

        // Pre-build empty lists OUTSIDE the when() chain — resourceList() internally calls
        // mock()/when(), which Mockito doesn't allow during an in-flight stubbing.
        KubernetesResourceList emptyPodSets = resourceList(List.of());
        KubernetesResourceList emptyProxies = resourceList(List.of());
        KubernetesResourceList emptyClusters = resourceList(List.of());

        podSetOp = mock(MixedOperation.class);
        podSetList = mock(AnyNamespaceOperation.class);
        when(client.resources(KafkaPodSet.class)).thenReturn(podSetOp);
        when(podSetOp.inAnyNamespace()).thenReturn(podSetList);
        when(podSetList.list()).thenReturn(emptyPodSets);

        proxyOp = mock(MixedOperation.class);
        proxyList = mock(AnyNamespaceOperation.class);
        when(client.resources(KafkaProxy.class)).thenReturn(proxyOp);
        when(proxyOp.inAnyNamespace()).thenReturn(proxyList);
        when(proxyList.list()).thenReturn(emptyProxies);

        clusterOp = mock(MixedOperation.class);
        clusterList = mock(AnyNamespaceOperation.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterOp);
        when(clusterOp.inAnyNamespace()).thenReturn(clusterList);
        when(clusterList.list()).thenReturn(emptyClusters);

        // Deployment chain — tests override what .get() returns
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
    void noProxies_returnsIdle() {
        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
        assertThat(json).contains("\"clusterId\":\"A\"");
    }

    @Test
    void targetProxyReady_returnsIdle() {
        KafkaProxy kp = mcsProxy(List.of("A", "B"));
        givenProxies(kp);
        when(namedDep.get()).thenReturn(deployment(/*generation*/ 1L, /*observed*/ 1L,
                /*desired*/ 1, /*updated*/ 1, /*available*/ 1));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
    }

    @Test
    void targetProxyMidRoll_returnsRolling() {
        KafkaProxy kp = mcsProxy(List.of("A", "B"));
        givenProxies(kp);
        // observedGeneration hasn't caught up — Kubernetes hasn't seen the new spec yet.
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 1, 1));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    @Test
    void targetProxyMidRoll_unavailableReplicas_returnsRolling() {
        KafkaProxy kp = mcsProxy(List.of("A", "B"));
        givenProxies(kp);
        // Old pod terminated, new one not yet ready.
        when(namedDep.get()).thenReturn(deployment(1L, 1L, 1, 1, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    @Test
    void proxyNotTargetingLocalCluster_doesNotFlipPhase() {
        // mcs.enabled=true and targetClusters=[B] — local cluster A is SKIPPED.
        // Even if SOME deployment exists with stale generation, this proxy must NOT contribute
        // to ROLLING, otherwise cluster B (rolling its own proxy) would block cluster C unfairly.
        KafkaProxy kp = mcsProxy(List.of("B"));
        givenProxies(kp);
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 0, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"IDLE\"");
    }

    @Test
    void nonMcsProxy_alwaysCountedAsTarget() {
        // mcs is null/disabled: single-cluster mode, the proxy is local to this cluster by
        // definition. A mid-roll must surface as ROLLING.
        KafkaProxy kp = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-proxy");
        meta.setNamespace(NS);
        kp.setMetadata(meta);
        kp.setSpec(new KafkaProxySpec());
        givenProxies(kp);
        when(namedDep.get()).thenReturn(deployment(2L, 1L, 1, 0, 0));

        String json = resource.get();

        assertThat(json).contains("\"upgradePhase\":\"ROLLING\"");
    }

    // --- helpers ---

    private void givenProxies(KafkaProxy... proxies) {
        KubernetesResourceList list = resourceList(List.of(proxies));
        when(proxyList.list()).thenReturn(list);
    }

    /** Mocks a KubernetesResourceList with the given items — fabric8 doesn't ship a public
     *  list class for our CRDs (only generic types), and we don't care about the other
     *  metadata so a stub mock with just .getItems() is sufficient. */
    private <T extends io.fabric8.kubernetes.api.model.HasMetadata> KubernetesResourceList<T> resourceList(List<T> items) {
        KubernetesResourceList<T> list = mock(KubernetesResourceList.class);
        when(list.getItems()).thenReturn(items);
        return list;
    }

    private KafkaProxy mcsProxy(List<String> targetClusters) {
        KafkaProxy kp = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-proxy");
        meta.setNamespace(NS);
        kp.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        spec.setMcs(mcs);
        spec.setTargetClusters(targetClusters);
        kp.setSpec(spec);
        return kp;
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
