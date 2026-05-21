package se.afshin.yavari.kafka.operator.proxy;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.KafkaCluster;
import se.afshin.yavari.kafka.operator.crd.KafkaClusterSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyMtlsConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaProxyReconcilerTest {

    private static final String NS = "kafka";
    private static final String PROXY_NAME = "my-proxy";
    private static final String POOL_NAME = "brokers-a";

    private KubernetesClient client;
    private KroxyliciousConfigBuilder configBuilder;
    private ProxyDeploymentBuilder deploymentBuilder;
    private ProxyServiceBuilder serviceBuilder;
    private ExternalAccessResolver externalAccessResolver;
    private TLSRouteBuilder tlsRouteBuilder;
    private IngressBuilder ingressBuilder;
    private se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator rollCoordinator;
    private Context<KafkaProxy> context;
    private KafkaProxyReconciler reconciler;

    // client chains
    private MixedOperation poolOp;
    private NonNamespaceOperation nsPoolOp;
    private Resource namedPoolOp;

    private MixedOperation cmOp;
    private NonNamespaceOperation nsCmOp;
    private Resource cmResource;

    private AppsAPIGroupDSL appsApi;
    private MixedOperation depOp;
    private NonNamespaceOperation nsDepOp;
    private RollableScalableResource depResource;
    private RollableScalableResource namedDep;

    private MixedOperation svcOp;
    private NonNamespaceOperation nsSvcOp;
    private ServiceResource svcResource;
    private ServiceResource namedSvc;

    private MixedOperation podOp;
    private NonNamespaceOperation nsPodOp;
    private FilterWatchListDeletable labeledPodOp;

    @BeforeEach
    void setup() throws Exception {
        configBuilder = mock(KroxyliciousConfigBuilder.class);
        deploymentBuilder = mock(ProxyDeploymentBuilder.class);
        serviceBuilder = mock(ProxyServiceBuilder.class);
        rollCoordinator = mock(se.afshin.yavari.kafka.operator.rolling.CrossClusterRollCoordinator.class);
        // Default: every coordinator query returns "my turn", so tests that don't care about
        // ordering behave exactly as before. The gate tests override this per-test.
        when(rollCoordinator.isMyTurnToRoll(any(), anyString())).thenReturn(true);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        // KafkaNodePool chain
        poolOp = mock(MixedOperation.class);
        nsPoolOp = mock(NonNamespaceOperation.class);
        namedPoolOp = mock(Resource.class);
        when(client.resources(KafkaNodePool.class)).thenReturn(poolOp);
        when(poolOp.inNamespace(NS)).thenReturn(nsPoolOp);
        when(nsPoolOp.withName(POOL_NAME)).thenReturn(namedPoolOp);
        when(namedPoolOp.get()).thenReturn(pool(1));

        // KafkaCluster chain (reconciler reads spec.proxyMtls from the parent cluster)
        MixedOperation clusterOp = mock(MixedOperation.class);
        NonNamespaceOperation nsClusterOp = mock(NonNamespaceOperation.class);
        Resource namedClusterOp = mock(Resource.class);
        when(client.resources(KafkaCluster.class)).thenReturn(clusterOp);
        when(clusterOp.inNamespace(NS)).thenReturn(nsClusterOp);
        when(nsClusterOp.withName(anyString())).thenReturn(namedClusterOp);
        when(namedClusterOp.get()).thenReturn(clusterWithProxyMtls());

        // Secrets chain — the reconciler checks that proxy client + server cert secrets exist.
        MixedOperation secretsOp = mock(MixedOperation.class);
        NonNamespaceOperation nsSecretsOp = mock(NonNamespaceOperation.class);
        Resource secretResource = mock(Resource.class);
        when(client.secrets()).thenReturn(secretsOp);
        when(secretsOp.inNamespace(NS)).thenReturn(nsSecretsOp);
        when(nsSecretsOp.withName(anyString())).thenReturn(secretResource);
        when(secretResource.get()).thenReturn(new io.fabric8.kubernetes.api.model.Secret());

        // ConfigMap chain
        cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        cmResource = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.resource(any(ConfigMap.class))).thenReturn(cmResource);
        when(nsCmOp.withName(anyString())).thenReturn(cmResource);

        // Deployment chain
        appsApi = mock(AppsAPIGroupDSL.class);
        depOp = mock(MixedOperation.class);
        nsDepOp = mock(NonNamespaceOperation.class);
        depResource = mock(RollableScalableResource.class);
        namedDep = mock(RollableScalableResource.class);
        when(client.apps()).thenReturn(appsApi);
        when(appsApi.deployments()).thenReturn(depOp);
        when(depOp.inNamespace(NS)).thenReturn(nsDepOp);
        when(nsDepOp.resource(any(Deployment.class))).thenReturn(depResource);
        when(nsDepOp.withName(PROXY_NAME)).thenReturn(namedDep);

        // Deployment ready status
        Deployment readyDeployment = new Deployment();
        DeploymentStatus depStatus = new DeploymentStatus();
        depStatus.setReadyReplicas(1);
        readyDeployment.setStatus(depStatus);
        when(namedDep.get()).thenReturn(readyDeployment);

        // Service chain
        svcOp = mock(MixedOperation.class);
        nsSvcOp = mock(NonNamespaceOperation.class);
        svcResource = mock(ServiceResource.class);
        namedSvc = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(Service.class))).thenReturn(svcResource);
        when(nsSvcOp.withName(anyString())).thenReturn(namedSvc);

        // Pod chain (for resolveNodeIdBase)
        podOp = mock(MixedOperation.class);
        nsPodOp = mock(NonNamespaceOperation.class);
        labeledPodOp = mock(FilterWatchListDeletable.class);
        PodList emptyPodList = new PodList();
        emptyPodList.setItems(List.of());
        when(client.pods()).thenReturn(podOp);
        when(podOp.inNamespace(NS)).thenReturn(nsPodOp);
        when(nsPodOp.withLabel(anyString(), anyString())).thenReturn(labeledPodOp);
        when(labeledPodOp.list()).thenReturn(emptyPodList);

        // Stubs
        when(configBuilder.build(any(), anyInt(), anyInt(), anyString(), anyBoolean(),
                any(ExternalAccessResolution.class))).thenReturn("config: {}");
        when(deploymentBuilder.build(any(), anyString(), anyString())).thenReturn(new Deployment());
        when(serviceBuilder.build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class)))
                .thenReturn(new Service());

        // Default resolver: no externalAccess → internal.
        externalAccessResolver = mock(ExternalAccessResolver.class);
        when(externalAccessResolver.resolve(any(), anyString(), anyString(), any()))
                .thenReturn(ExternalAccessResolution.internal());

        tlsRouteBuilder = mock(TLSRouteBuilder.class);
        ingressBuilder = mock(IngressBuilder.class);

        reconciler = new KafkaProxyReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "configBuilder", configBuilder);
        injectField(reconciler, "deploymentBuilder", deploymentBuilder);
        injectField(reconciler, "serviceBuilder", serviceBuilder);
        injectField(reconciler, "externalAccessResolver", externalAccessResolver);
        injectField(reconciler, "tlsRouteBuilder", tlsRouteBuilder);
        injectField(reconciler, "ingressBuilder", ingressBuilder);
        injectField(reconciler, "rollCoordinator", rollCoordinator);
        injectField(reconciler, "rollTracker", new ProxyRollTracker());
        var secretRevisionTracker =
                mock(se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker.class);
        when(secretRevisionTracker.revisionsOf(any(), anyString())).thenReturn("");
        injectField(reconciler, "secretRevisionTracker", secretRevisionTracker);
        injectField(reconciler, "localClusterId", "A");
    }

    @Test
    void poolNotFound_reschedules() {
        when(namedPoolOp.get()).thenReturn(null);
        KafkaProxy proxy = proxy(null);

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.RECONCILING);
        assertThat(proxy.getStatus().getMessage()).contains(POOL_NAME);
        assertThat(result.getScheduleDelay()).isPresent();
    }

    @Test
    void happyPath_noApicurio_setsReady() {
        KafkaProxy proxy = proxy(null);

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.READY);
        verify(cmResource).createOrReplace();
        verify(depResource).serverSideApply();
        verify(svcResource).serverSideApply();
    }

    @Test
    void gatewayMode_invokesTlsRouteBuilder() {
        // External access type=GATEWAY → reconciler calls TLSRouteBuilder. Missing CRD on the
        // mock client is swallowed by applyTlsRoute (mirrors ServiceExport pattern), so the
        // build invocation is the signal we verify.
        when(externalAccessResolver.resolve(any(), anyString(), anyString(), any()))
                .thenReturn(ExternalAccessResolution.resolved(
                        se.afshin.yavari.kafka.operator.crd.ExternalAccessType.GATEWAY,
                        "a.kafka.example.com"));
        KafkaProxy proxy = proxy(null);

        reconciler.reconcile(proxy, context);

        verify(tlsRouteBuilder).build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class));
    }

    @Test
    void ingressMode_invokesIngressBuilder() {
        when(externalAccessResolver.resolve(any(), anyString(), anyString(), any()))
                .thenReturn(ExternalAccessResolution.resolved(
                        se.afshin.yavari.kafka.operator.crd.ExternalAccessType.INGRESS,
                        "a.kafka.example.com"));
        KafkaProxy proxy = proxy(null);

        reconciler.reconcile(proxy, context);

        verify(ingressBuilder).build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class));
        verify(tlsRouteBuilder, org.mockito.Mockito.never())
                .build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class));
    }

    @Test
    void loadBalancerMode_doesNotInvokeRouteOrIngressBuilders() {
        // Only GATEWAY/INGRESS modes trigger the route/ingress paths.
        when(externalAccessResolver.resolve(any(), anyString(), anyString(), any()))
                .thenReturn(ExternalAccessResolution.resolved(
                        se.afshin.yavari.kafka.operator.crd.ExternalAccessType.LOADBALANCER,
                        "10.0.0.5"));
        KafkaProxy proxy = proxy(null);

        reconciler.reconcile(proxy, context);

        verify(tlsRouteBuilder, org.mockito.Mockito.never())
                .build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class));
        verify(ingressBuilder, org.mockito.Mockito.never())
                .build(any(), anyInt(), anyString(), any(ExternalAccessResolution.class));
    }

    @Test
    void loadBalancerPending_appliesServiceAndReschedules() {
        // External access configured but LB ingress not yet allocated. We still apply the
        // Service (so cloud-controller starts provisioning) and reschedule with a status hint.
        when(externalAccessResolver.resolve(any(), anyString(), anyString(), any()))
                .thenReturn(ExternalAccessResolution.pending(
                        se.afshin.yavari.kafka.operator.crd.ExternalAccessType.LOADBALANCER));
        KafkaProxy proxy = proxy(null);

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(result.getScheduleDelay()).isPresent();
        assertThat(proxy.getStatus().getMessage()).contains("LoadBalancer");
        // Service must still be applied so the LB starts provisioning.
        verify(svcResource).serverSideApply();
    }

    @Test
    void cleanup_deletesConfigMapDeploymentService() {
        KafkaProxy proxy = proxy(null);

        reconciler.cleanup(proxy, context);

        verify(cmResource).delete();
        verify(namedDep).delete();
        verify(namedSvc).delete();
    }

    @Test
    void mcsEnabled_clusterInTargets_reconcilesNormally() {
        // localClusterId="A" (injected in setup), targets include A → deploy.
        KafkaProxy proxy = mcsProxy(List.of("A", "B"), List.of(
                range("brokers-a", 0, 0),
                range("brokers-b", 1000, 1000)));
        // Cluster CR must list A & B for the cross-ref check to pass.
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithProxyMtlsAndIds("A", "B"));

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.READY);
        verify(cmResource).createOrReplace();
        verify(depResource).serverSideApply();
        verify(svcResource).serverSideApply();
    }

    @Test
    void mcsEnabled_clusterNotInTargets_setsSkipped() {
        // localClusterId="A", targets only B → skip, no resources reconciled.
        KafkaProxy proxy = mcsProxy(List.of("B"), List.of(range("brokers-b", 1000, 1000)));

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.SKIPPED);
        assertThat(proxy.getStatus().getMessage()).contains("'A' is not a target");
        // No deployment/service/configmap creation calls.
        verify(cmResource, org.mockito.Mockito.never()).createOrReplace();
        verify(depResource, org.mockito.Mockito.never()).serverSideApply();
        verify(svcResource, org.mockito.Mockito.never()).serverSideApply();
    }

    @Test
    void mcsEnabled_emptyTargetClusters_fails() {
        KafkaProxy proxy = mcsProxy(List.of(), List.of(range("brokers-a", 0, 0)));

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.FAILED);
        assertThat(proxy.getStatus().getMessage()).contains("targetClusters");
    }

    @Test
    void mcsEnabled_emptyBrokerNodeIdRanges_fails() {
        KafkaProxy proxy = mcsProxy(List.of("A"), List.of());

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.FAILED);
        assertThat(proxy.getStatus().getMessage()).contains("brokerNodeIdRanges");
    }

    @Test
    void mcsEnabled_unknownTargetClusterId_fails() {
        KafkaProxy proxy = mcsProxy(List.of("A", "ZZ"), List.of(range("brokers-a", 0, 0)));
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithProxyMtlsAndIds("A", "B"));

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.FAILED);
        assertThat(proxy.getStatus().getMessage()).contains("'ZZ'");
    }

    @Test
    void targetClustersSetButMcsDisabled_fails() {
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setTargetClusters(List.of("A"));
        // mcs is null/disabled (proxy() helper does not set it).

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.FAILED);
        assertThat(proxy.getStatus().getMessage()).contains("spec.targetClusters");
    }

    @Test
    void rollGate_clusterRollOrderUnset_proceedsWithoutGate() throws Exception {
        // Even with an existing Deployment + image difference, the gate is dormant if
        // clusterRollOrder isn't set. Coordinator should not even be consulted.
        when(namedDep.get()).thenReturn(deploymentWithImage("kroxy-filters:OLD", "deadbeef00aa"));
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setImage("kroxy-filters:NEW");

        reconciler.reconcile(proxy, context);

        verify(rollCoordinator, org.mockito.Mockito.never()).isMyTurnToRoll(any(), anyString());
        verify(depResource).serverSideApply();
    }

    @Test
    void rollGate_firstTimeDeploy_proceedsBecauseNoExisting() throws Exception {
        // No existing Deployment → initial deploy, not an upgrade → no gate.
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithRollOrder("A", "B"));
        when(namedDep.get()).thenReturn(null);
        KafkaProxy proxy = proxy(null);

        reconciler.reconcile(proxy, context);

        verify(rollCoordinator, org.mockito.Mockito.never()).isMyTurnToRoll(any(), anyString());
        verify(depResource).serverSideApply();
    }

    @Test
    void rollGate_imageChange_myTurn_proceeds() throws Exception {
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithRollOrder("A", "B"));
        // existing image differs from the new spec.image → roll required.
        when(namedDep.get()).thenReturn(deploymentWithImage("kroxy-filters:OLD", sha12("config: {}")));
        when(rollCoordinator.isMyTurnToRoll(any(), anyString())).thenReturn(true);
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setImage("kroxy-filters:NEW");

        reconciler.reconcile(proxy, context);

        verify(rollCoordinator).isMyTurnToRoll(any(), anyString());
        verify(depResource).serverSideApply();
    }

    @Test
    void rollGate_imageChange_notMyTurn_reschedules() throws Exception {
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithRollOrder("A", "B"));
        when(namedDep.get()).thenReturn(deploymentWithImage("kroxy-filters:OLD", sha12("config: {}")));
        when(rollCoordinator.isMyTurnToRoll(any(), anyString())).thenReturn(false);
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setImage("kroxy-filters:NEW");

        var result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getMessage()).contains("Waiting for preceding cluster");
        assertThat(result.getScheduleDelay()).isPresent();
        // Critically: no Deployment apply happened.
        verify(depResource, org.mockito.Mockito.never()).serverSideApply();
    }

    @Test
    void rollGate_configHashChange_notMyTurn_reschedules() throws Exception {
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithRollOrder("A", "B"));
        // Existing has same image but a stale hash → still a roll.
        when(namedDep.get()).thenReturn(deploymentWithImage("kroxy-filters:dev", "stalehash00"));
        when(configBuilder.build(any(), anyInt(), anyInt(), anyString(), anyBoolean(),
                any(ExternalAccessResolution.class))).thenReturn("config: {fresh}");
        when(rollCoordinator.isMyTurnToRoll(any(), anyString())).thenReturn(false);
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setImage("kroxy-filters:dev");

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getMessage()).contains("Waiting for preceding cluster");
        verify(depResource, org.mockito.Mockito.never()).serverSideApply();
    }

    @Test
    void rollGate_imageAndHashUnchanged_proceedsWithoutConsultingCoordinator() throws Exception {
        when(client.resources(KafkaCluster.class).inNamespace(NS).withName(anyString()).get())
                .thenReturn(clusterWithRollOrder("A", "B"));
        when(namedDep.get()).thenReturn(deploymentWithImage("kroxy-filters:dev", sha12("config: {}")));
        KafkaProxy proxy = proxy(null);
        proxy.getSpec().setImage("kroxy-filters:dev");

        reconciler.reconcile(proxy, context);

        // Idempotent reconcile: image and hash both match → rollIsRequired=false → gate skipped.
        verify(rollCoordinator, org.mockito.Mockito.never()).isMyTurnToRoll(any(), anyString());
        verify(depResource).serverSideApply();
    }

    @Test
    void mcsEnabled_skipPath_doesNotRequireLocalPool() {
        // SKIPPED clusters should not even try to look up the pool — this asserts the early-return.
        when(namedPoolOp.get()).thenReturn(null);
        KafkaProxy proxy = mcsProxy(List.of("B"), List.of(range("brokers-b", 1000, 1000)));

        reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.SKIPPED);
    }

    // --- helpers ---

    private KafkaProxy proxy(String apicurioRef) {
        KafkaProxy p = new KafkaProxy();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(PROXY_NAME);
        meta.setNamespace(NS);
        p.setMetadata(meta);
        KafkaProxySpec spec = new KafkaProxySpec();
        spec.setPoolRef(POOL_NAME);
        spec.setClusterRef("kafka-a");
        spec.setClientPort(9094);
        spec.setReplicas(1);
        spec.setApicurioRef(apicurioRef);
        p.setSpec(spec);
        return p;
    }

    private KafkaCluster clusterWithProxyMtls() {
        KafkaCluster c = new KafkaCluster();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("kafka-a");
        meta.setNamespace(NS);
        c.setMetadata(meta);
        KafkaClusterSpec spec = new KafkaClusterSpec();
        KafkaProxyMtlsConfig pm = new KafkaProxyMtlsConfig();
        pm.setEnabled(true);
        pm.setProxyPrincipal("kafka-proxy");
        spec.setProxyMtls(pm);
        c.setSpec(spec);
        return c;
    }

    private KafkaProxy mcsProxy(List<String> targetClusters, List<se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange> ranges) {
        KafkaProxy p = proxy(null);
        se.afshin.yavari.kafka.operator.crd.McsConfig mcs = new se.afshin.yavari.kafka.operator.crd.McsConfig();
        mcs.setEnabled(true);
        p.getSpec().setMcs(mcs);
        p.getSpec().setTargetClusters(targetClusters);
        p.getSpec().setBrokerNodeIdRanges(ranges);
        return p;
    }

    private se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange range(String name, int start, int end) {
        se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange r = new se.afshin.yavari.kafka.operator.crd.BrokerNodeIdRange();
        r.setName(name);
        r.setStart(start);
        r.setEnd(end);
        return r;
    }

    private KafkaCluster clusterWithRollOrder(String... order) {
        KafkaCluster c = clusterWithProxyMtls();
        c.getSpec().setClusterRollOrder(java.util.Arrays.asList(order));
        // clusters[] must include every id in clusterRollOrder for the rollCoordinator's
        // map lookup. We populate ClusterEntries with non-empty addresses so the real
        // coordinator (when used) would still work; mocked rollCoordinator ignores this.
        java.util.List<se.afshin.yavari.kafka.operator.crd.ClusterEntry> entries = new java.util.ArrayList<>();
        for (String id : order) {
            se.afshin.yavari.kafka.operator.crd.ClusterEntry e = new se.afshin.yavari.kafka.operator.crd.ClusterEntry();
            e.setId(id);
            e.setOperatorAddress("kafka-operator-" + id.toLowerCase() + ".kafka.svc.clusterset.local:8080");
            entries.add(e);
        }
        c.getSpec().setClusters(entries);
        return c;
    }

    private Deployment deploymentWithImage(String image, String configHash) {
        Deployment d = new Deployment();
        d.setSpec(new io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withAnnotations(java.util.Map.of(
                                ProxyDeploymentBuilder.CONFIG_HASH_ANNOTATION, configHash))
                    .endMetadata()
                    .withNewSpec()
                        .addToContainers(new io.fabric8.kubernetes.api.model.ContainerBuilder()
                                .withName("kroxylicious")
                                .withImage(image)
                                .build())
                    .endSpec()
                .endTemplate()
                .build());
        DeploymentStatus s = new DeploymentStatus();
        s.setReadyReplicas(1);
        d.setStatus(s);
        return d;
    }

    /** Mirror of the reconciler's configHash inputs. Tests inject an empty secret-revision
     *  string (see setup()), so we pass "" as the second part to match. */
    private static String sha12(String configYaml) {
        return se.afshin.yavari.kafka.operator.infra.ConfigHasher.sha256(configYaml, "");
    }

    private KafkaCluster clusterWithProxyMtlsAndIds(String... ids) {
        KafkaCluster c = clusterWithProxyMtls();
        java.util.List<se.afshin.yavari.kafka.operator.crd.ClusterEntry> entries = new java.util.ArrayList<>();
        for (String id : ids) {
            se.afshin.yavari.kafka.operator.crd.ClusterEntry e = new se.afshin.yavari.kafka.operator.crd.ClusterEntry();
            e.setId(id);
            entries.add(e);
        }
        c.getSpec().setClusters(entries);
        return c;
    }

    private KafkaNodePool pool(int replicas) {
        KafkaNodePool pool = new KafkaNodePool();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(POOL_NAME);
        meta.setNamespace(NS);
        pool.setMetadata(meta);
        KafkaNodePoolSpec spec = new KafkaNodePoolSpec();
        spec.setRoles(List.of(NodeRole.BROKER));
        spec.setReplicas(replicas);
        pool.setSpec(spec);
        return pool;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void referencesSecret_defaultNames_matches() {
        KafkaProxy proxy = proxy(null);  // no tls override → defaults to {name}-client-tls + -server-tls
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, PROXY_NAME + "-client-tls")).isTrue();
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, PROXY_NAME + "-server-tls")).isTrue();
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, "unrelated-tls")).isFalse();
    }

    @Test
    void referencesSecret_overriddenNames_matchesOverride() {
        KafkaProxy proxy = proxy(null);
        var tls = new se.afshin.yavari.kafka.operator.crd.KafkaProxyTlsConfig();
        tls.setClientCertSecretRef("custom-client");
        tls.setServerCertSecretRef("custom-server");
        proxy.getSpec().setTls(tls);
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, "custom-client")).isTrue();
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, "custom-server")).isTrue();
        assertThat(KafkaProxyReconciler.referencesSecret(proxy, PROXY_NAME + "-client-tls")).isFalse();
    }
}
