package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerIngressBuilder;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistrySpec;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStorageConfig;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.McsConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpGatewayConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressBuilder;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpRouteBuilder;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ApicurioRegistryReconcilerTest {

    private static final String NS = "kafka";
    private static final String REGISTRY_NAME = "my-apicurio";
    private static final String RBAC_REF = "my-rbac";

    private KubernetesClient client;
    private ApicurioDeploymentBuilder deploymentBuilder;
    private ApicurioProxyContainerBuilder proxyContainerBuilder;
    private ApicurioProxyServiceBuilder proxyServiceBuilder;
    private ApicurioKafkasqlSupport kafkasqlSupport;
    private ExternalAccessResolver externalAccessResolver;
    private HttpIngressBuilder httpIngressBuilder;
    private HttpRouteBuilder httpRouteBuilder;
    private se.afshin.yavari.kafka.operator.infra.ServiceExportManager serviceExportManager;
    private se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier optionalApplier;
    private Context<ApicurioRegistry> context;
    private ApicurioRegistryReconciler reconciler;

    // client chains
    private MixedOperation cmOp;
    private NonNamespaceOperation nsCmOp;
    private Resource namedCmOp;

    private AppsAPIGroupDSL appsApi;
    private MixedOperation depOp;
    private NonNamespaceOperation nsDepOp;
    private RollableScalableResource depResource;
    private RollableScalableResource namedDep;

    private MixedOperation svcOp;
    private NonNamespaceOperation nsSvcOp;
    private ServiceResource svcResource;
    private ServiceResource namedSvc;

    private Resource ingResource;
    private Resource routeResource;
    private Resource serviceExportResource;

    @BeforeEach
    void setup() throws Exception {
        deploymentBuilder = mock(ApicurioDeploymentBuilder.class);
        proxyContainerBuilder = mock(ApicurioProxyContainerBuilder.class);
        proxyServiceBuilder = mock(ApicurioProxyServiceBuilder.class);
        kafkasqlSupport = mock(ApicurioKafkasqlSupport.class);
        externalAccessResolver = new ExternalAccessResolver();
        httpIngressBuilder = mock(HttpIngressBuilder.class);
        httpRouteBuilder = mock(HttpRouteBuilder.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        // ConfigMap chain (for RBAC policy check)
        cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        namedCmOp = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.withName(anyString())).thenReturn(namedCmOp);
        ConfigMap policyMap = new ConfigMap();
        when(namedCmOp.get()).thenReturn(policyMap);

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
        when(nsDepOp.withName(anyString())).thenReturn(namedDep);

        Deployment readyDep = new Deployment();
        DeploymentStatus ds = new DeploymentStatus();
        ds.setReadyReplicas(1);
        readyDep.setStatus(ds);
        when(namedDep.get()).thenReturn(readyDep);

        // Service chain — `withName` used by the external-access resolver to read LB status.
        svcOp = mock(MixedOperation.class);
        nsSvcOp = mock(NonNamespaceOperation.class);
        svcResource = mock(ServiceResource.class);
        namedSvc = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(Service.class))).thenReturn(svcResource);
        when(nsSvcOp.withName(anyString())).thenReturn(namedSvc);
        // Default: LB Service has a resolved ingress IP.
        when(namedSvc.get()).thenReturn(svcWithLb("198.51.100.10"));

        // Networking / Ingress chain
        NetworkAPIGroupDSL netApi = mock(NetworkAPIGroupDSL.class);
        V1NetworkAPIGroupDSL v1NetApi = mock(V1NetworkAPIGroupDSL.class);
        when(client.network()).thenReturn(netApi);
        when(netApi.v1()).thenReturn(v1NetApi);
        MixedOperation ingOp = mock(MixedOperation.class);
        NonNamespaceOperation nsIngOp = mock(NonNamespaceOperation.class);
        ingResource = mock(Resource.class);
        when(v1NetApi.ingresses()).thenReturn(ingOp);
        when(ingOp.inNamespace(NS)).thenReturn(nsIngOp);
        when(nsIngOp.resource(any(Ingress.class))).thenReturn(ingResource);
        when(nsIngOp.withName(anyString())).thenReturn(ingResource);

        // GenericKubernetesResource (HTTPRoute) chain
        MixedOperation routeMixed = mock(MixedOperation.class);
        NonNamespaceOperation routeNamespaced = mock(NonNamespaceOperation.class);
        routeResource = mock(Resource.class);
        when(client.genericKubernetesResources(HttpRouteBuilder.API_VERSION, HttpRouteBuilder.KIND))
                .thenReturn(routeMixed);
        when(routeMixed.inNamespace(NS)).thenReturn(routeNamespaced);
        when(routeNamespaced.resource(any(GenericKubernetesResource.class))).thenReturn(routeResource);
        when(routeNamespaced.withName(anyString())).thenReturn(routeResource);

        // GenericKubernetesResource (ServiceExport) chain
        MixedOperation seMixed = mock(MixedOperation.class);
        NonNamespaceOperation seNamespaced = mock(NonNamespaceOperation.class);
        serviceExportResource = mock(Resource.class);
        when(client.genericKubernetesResources("multicluster.x-k8s.io/v1alpha1", "ServiceExport"))
                .thenReturn(seMixed);
        when(seMixed.inNamespace(NS)).thenReturn(seNamespaced);
        when(seNamespaced.resource(any(GenericKubernetesResource.class))).thenReturn(serviceExportResource);

        // Stubs
        when(deploymentBuilder.build(any(), anyString(), any(), any(), any(), anyString())).thenReturn(new Deployment());
        when(proxyContainerBuilder.build(any())).thenReturn(new Container());
        when(proxyContainerBuilder.policyVolume(anyString())).thenReturn(new Volume());
        when(proxyServiceBuilder.build(any(), anyString())).thenReturn(new Service());
        when(httpIngressBuilder.build(anyString(), anyString(), any(), any(), anyString(),
                anyString(), anyInt(), any())).thenReturn(new Ingress());
        when(httpRouteBuilder.build(anyString(), anyString(), any(), any(), anyString(),
                anyString(), anyInt(), any())).thenReturn(new GenericKubernetesResource());

        reconciler = new ApicurioRegistryReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "deploymentBuilder", deploymentBuilder);
        injectField(reconciler, "proxyContainerBuilder", proxyContainerBuilder);
        injectField(reconciler, "proxyServiceBuilder", proxyServiceBuilder);
        injectField(reconciler, "kafkasqlSupport", kafkasqlSupport);
        injectField(reconciler, "externalAccessResolver", externalAccessResolver);
        injectField(reconciler, "httpIngressBuilder", httpIngressBuilder);
        injectField(reconciler, "httpRouteBuilder", httpRouteBuilder);
        var secretRevisionTracker =
                mock(se.afshin.yavari.kafka.operator.infra.SecretRevisionTracker.class);
        when(secretRevisionTracker.revisionsOf(any(), anyString())).thenReturn("");
        injectField(reconciler, "secretRevisionTracker", secretRevisionTracker);
        serviceExportManager = mock(se.afshin.yavari.kafka.operator.infra.ServiceExportManager.class);
        injectField(reconciler, "serviceExportManager", serviceExportManager);
        optionalApplier = mock(se.afshin.yavari.kafka.operator.infra.OptionalResourceApplier.class);
        injectField(reconciler, "optionalApplier", optionalApplier);
        injectField(reconciler, "mcsEnabled", false);
        injectField(reconciler, "localClusterId", "A");
    }

    @Test
    void rbacCmNotReady_setsReconciling() {
        when(namedCmOp.get()).thenReturn(null);
        ApicurioRegistry registry = registry(RBAC_REF, null);

        UpdateControl<ApicurioRegistry> result = reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.RECONCILING);
        assertThat(registry.getStatus().getMessage()).contains(RBAC_REF);
        assertThat(result.getScheduleDelay()).isPresent();
    }

    @Test
    void noProxy_deploysSingleContainerNoProxyService() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.reconcile(registry, context);

        verify(proxyContainerBuilder, never()).build(any());
        verify(proxyContainerBuilder, never()).policyVolume(anyString());
        verify(proxyServiceBuilder, never()).build(any(), anyString());
        verify(depResource, times(1)).serverSideApply();
        verify(svcResource, never()).serverSideApply();
    }

    @Test
    void withProxy_deploysMergedPodAndProxyService() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");

        reconciler.reconcile(registry, context);

        verify(proxyContainerBuilder).build(any());
        verify(proxyContainerBuilder).policyVolume(RBAC_REF);
        verify(deploymentBuilder).build(any(), anyString(), any(Container.class), any(Volume.class), any(), anyString());
        verify(proxyServiceBuilder).build(any(), anyString());
        verify(depResource, times(1)).serverSideApply();
        verify(svcResource, times(1)).serverSideApply();
    }

    @Test
    void withProxy_setsProxyUrlInStatus() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getProxyUrl())
                .contains(REGISTRY_NAME + "-rbac-proxy")
                .contains(NS)
                .contains(String.valueOf(ApicurioProxyContainerBuilder.PROXY_PORT));
        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.READY);
    }

    @Test
    void noProxy_doesNotSetProxyUrl() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getProxyUrl()).isNull();
        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.READY);
    }

    @Test
    void externalAccessWithoutRbacRef_setsFailed() {
        ApicurioRegistry registry = registry(null, null);
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.LOADBALANCER);
        registry.getSpec().setExternalAccess(ea);

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.FAILED);
        assertThat(registry.getStatus().getMessage()).contains("rbacRef");
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void externalAccessLoadBalancer_setsExternalUrl() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.LOADBALANCER);
        registry.getSpec().setExternalAccess(ea);

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getExternalUrl())
                .isEqualTo("http://198.51.100.10:" + ApicurioProxyContainerBuilder.PROXY_PORT);
        // Ingress / HTTPRoute should NOT be applied for LOADBALANCER type.
        verify(httpIngressBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpRouteBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
    }

    @Test
    void externalAccessIngress_appliesIngress() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.INGRESS);
        ea.setAdvertisedHostTemplate("schema-${clusterId}.example.com");
        HttpIngressConfig ing = new HttpIngressConfig();
        ing.setIngressClassName("nginx");
        ea.setIngress(ing);
        registry.getSpec().setExternalAccess(ea);

        reconciler.reconcile(registry, context);

        verify(httpIngressBuilder, times(1)).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpRouteBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        assertThat(registry.getStatus().getExternalUrl()).startsWith("http://schema-a.example.com");
    }

    @Test
    void externalAccessGateway_appliesHttpRoute() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.GATEWAY);
        ea.setAdvertisedHostTemplate("schema.example.com");
        HttpGatewayConfig gw = new HttpGatewayConfig();
        gw.setParentGatewayName("external-gw");
        ea.setGateway(gw);
        registry.getSpec().setExternalAccess(ea);

        reconciler.reconcile(registry, context);

        verify(httpRouteBuilder, times(1)).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpIngressBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        assertThat(registry.getStatus().getExternalUrl()).contains("schema.example.com");
    }

    @Test
    void kafkasql_pending_setsReconcilingAndReschedules() {
        ApicurioRegistry registry = kafkasqlRegistry();
        when(kafkasqlSupport.prepare(any())).thenReturn(
                new ApicurioKafkasqlSupport.Result.Pending("waiting for journal topic"));

        UpdateControl<ApicurioRegistry> result = reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.RECONCILING);
        assertThat(registry.getStatus().getMessage()).contains("waiting for journal topic");
        assertThat(result.getScheduleDelay()).isPresent();
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void kafkasql_failed_setsFailedStatus() {
        ApicurioRegistry registry = kafkasqlRegistry();
        when(kafkasqlSupport.prepare(any())).thenReturn(
                new ApicurioKafkasqlSupport.Result.Failed("clusterRef missing"));

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.FAILED);
        assertThat(registry.getStatus().getMessage()).contains("clusterRef missing");
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void kafkasql_ready_passesConfigToDeploymentBuilder() {
        ApicurioRegistry registry = kafkasqlRegistry();
        var cfg = new ApicurioDeploymentBuilder.KafkasqlConfig("b:9092", "kafkasql-journal", "registry-tls", "kafka-ubi:4.0.0");
        when(kafkasqlSupport.prepare(any())).thenReturn(new ApicurioKafkasqlSupport.Result.Ready(cfg));

        reconciler.reconcile(registry, context);

        verify(deploymentBuilder).build(any(), anyString(), any(), any(), org.mockito.ArgumentMatchers.eq(cfg), anyString());
    }

    @Test
    void cleanup_invokesKafkasqlCleanup() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.cleanup(registry, context);

        verify(kafkasqlSupport, times(1)).cleanup(registry);
    }

    @Test
    void cleanup_deletesMergedDeploymentAndProxyService() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.cleanup(registry, context);

        verify(namedDep, times(1)).delete();
        verify(namedSvc, times(1)).delete();
    }

    // --- MCS multi-cluster HA (#19) ---

    @Test
    void mcsEnabled_clusterNotInTargetClusters_setsSkipped() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        registry.getSpec().setMcs(mcs);
        registry.getSpec().setTargetClusters(List.of("B", "C"));

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.SKIPPED);
        assertThat(registry.getStatus().getMessage()).contains("'A'").contains("not a target");
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
        verify(proxyServiceBuilder, never()).build(any(), anyString());
    }

    @Test
    void mcsEnabled_emptyTargetClusters_setsFailed() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        registry.getSpec().setMcs(mcs);
        registry.getSpec().setTargetClusters(List.of());

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.FAILED);
        assertThat(registry.getStatus().getMessage()).contains("targetClusters");
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void mcsDisabled_butTargetClustersSet_setsFailed() {
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        registry.getSpec().setTargetClusters(List.of("A"));

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.FAILED);
        assertThat(registry.getStatus().getMessage()).contains("targetClusters").contains("mcs.enabled");
        verify(deploymentBuilder, never()).build(any(), anyString(), any(), any(), any(), anyString());
    }

    @Test
    void mcsEnabled_clusterInTargetClusters_appliesServiceExport() throws Exception {
        injectField(reconciler, "mcsEnabled", true); // operator-level MCS flag
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        registry.getSpec().setMcs(mcs);
        registry.getSpec().setTargetClusters(List.of("A", "B", "C"));

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.READY);
        verify(deploymentBuilder, times(1)).build(any(), anyString(), any(), any(), any(), anyString());
        verify(serviceExportManager, times(1))
                .apply(org.mockito.ArgumentMatchers.eq(REGISTRY_NAME + "-rbac-proxy"),
                        org.mockito.ArgumentMatchers.eq(NS));
    }

    // --- helpers ---

    private ApicurioRegistry registry(String rbacRef, String rbacProxyImage) {
        ApicurioRegistry r = new ApicurioRegistry();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(REGISTRY_NAME);
        meta.setNamespace(NS);
        r.setMetadata(meta);
        ApicurioRegistrySpec spec = new ApicurioRegistrySpec();
        spec.setRbacRef(rbacRef);
        spec.setRbacProxyImage(rbacProxyImage);
        spec.setReplicas(1);
        r.setSpec(spec);
        return r;
    }

    private ApicurioRegistry kafkasqlRegistry() {
        ApicurioRegistry r = registry(null, null);
        ApicurioRegistryStorageConfig storage = new ApicurioRegistryStorageConfig();
        storage.setType("kafkasql");
        storage.setClusterRef("my-kafka");
        storage.setKafkaTopic("kafkasql-journal");
        r.getSpec().setStorage(storage);
        return r;
    }

    private Service svcWithLb(String ip) {
        Service svc = new Service();
        ServiceStatus status = new ServiceStatus();
        LoadBalancerStatus lb = new LoadBalancerStatus();
        LoadBalancerIngress ing = new LoadBalancerIngressBuilder().withIp(ip).build();
        lb.setIngress(List.of(ing));
        status.setLoadBalancer(lb);
        svc.setStatus(status);
        return svc;
    }

    @Test
    void referencesSecret_kafkasqlTlsSecretRefMatch() {
        ApicurioRegistry r = registry(null, null);
        var storage = new ApicurioRegistryStorageConfig();
        storage.setTlsSecretRef("registry-tls");
        r.getSpec().setStorage(storage);
        assertThat(ApicurioRegistryReconciler.referencesSecret(r, "registry-tls")).isTrue();
        assertThat(ApicurioRegistryReconciler.referencesSecret(r, "unrelated-tls")).isFalse();
    }

    @Test
    void referencesSecret_noStorage_returnsFalse() {
        ApicurioRegistry r = registry(null, null);
        assertThat(ApicurioRegistryReconciler.referencesSecret(r, "anything")).isFalse();
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
