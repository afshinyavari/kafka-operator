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
import org.mockito.ArgumentCaptor;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistry;
import se.afshin.yavari.kafka.operator.crd.ApicurioRegistryStatus;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePool;
import se.afshin.yavari.kafka.operator.crd.KafkaNodePoolSpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxy;
import se.afshin.yavari.kafka.operator.crd.KafkaProxySpec;
import se.afshin.yavari.kafka.operator.crd.KafkaProxyStatus;
import se.afshin.yavari.kafka.operator.crd.NodeRole;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaProxyReconcilerTest {

    private static final String NS = "kafka";
    private static final String PROXY_NAME = "my-proxy";
    private static final String POOL_NAME = "brokers-a";
    private static final String APICURIO_NAME = "my-apicurio";

    private KubernetesClient client;
    private KroxyliciousConfigBuilder configBuilder;
    private ProxyDeploymentBuilder deploymentBuilder;
    private ProxyServiceBuilder serviceBuilder;
    private ProxyTlsManager proxyTlsManager;
    private Context<KafkaProxy> context;
    private KafkaProxyReconciler reconciler;

    // client chains
    private MixedOperation poolOp;
    private NonNamespaceOperation nsPoolOp;
    private Resource namedPoolOp;

    private MixedOperation apicurioOp;
    private NonNamespaceOperation nsApicurioOp;
    private Resource namedApicurioOp;

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
        proxyTlsManager = mock(ProxyTlsManager.class);
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

        // ApicurioRegistry chain
        apicurioOp = mock(MixedOperation.class);
        nsApicurioOp = mock(NonNamespaceOperation.class);
        namedApicurioOp = mock(Resource.class);
        when(client.resources(ApicurioRegistry.class)).thenReturn(apicurioOp);
        when(apicurioOp.inNamespace(NS)).thenReturn(nsApicurioOp);
        when(nsApicurioOp.withName(APICURIO_NAME)).thenReturn(namedApicurioOp);
        when(namedApicurioOp.get()).thenReturn(null);

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
        when(configBuilder.build(any(), anyInt(), anyInt(), any(), anyString())).thenReturn("config: {}");
        when(deploymentBuilder.build(any(), anyString())).thenReturn(new Deployment());
        when(serviceBuilder.build(any(), anyInt(), anyString())).thenReturn(new Service());

        reconciler = new KafkaProxyReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "configBuilder", configBuilder);
        injectField(reconciler, "deploymentBuilder", deploymentBuilder);
        injectField(reconciler, "serviceBuilder", serviceBuilder);
        injectField(reconciler, "proxyTlsManager", proxyTlsManager);
        injectField(reconciler, "mcsEnabled", false);
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
    void apicurioRefSet_registryNotReady_reschedules() {
        KafkaProxy proxy = proxy(APICURIO_NAME);
        // namedApicurioOp.get() returns null (not found) or no URL

        UpdateControl<KafkaProxy> result = reconciler.reconcile(proxy, context);

        assertThat(proxy.getStatus().getPhase()).isEqualTo(KafkaProxyStatus.Phase.RECONCILING);
        assertThat(proxy.getStatus().getMessage()).contains(APICURIO_NAME);
        assertThat(result.getScheduleDelay()).isPresent();
        verify(configBuilder, never()).build(any(), anyInt(), anyInt(), any(), anyString());
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
    void happyPath_withApicurio_passesUrlToConfigBuilder() {
        ApicurioRegistry apicurio = new ApicurioRegistry();
        ApicurioRegistryStatus aprStatus = new ApicurioRegistryStatus();
        aprStatus.setRegistryUrl("http://apicurio:8080");
        apicurio.setStatus(aprStatus);
        when(namedApicurioOp.get()).thenReturn(apicurio);

        KafkaProxy proxy = proxy(APICURIO_NAME);
        reconciler.reconcile(proxy, context);

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        verify(configBuilder).build(any(), anyInt(), anyInt(), urlCaptor.capture(), anyString());
        assertThat(urlCaptor.getValue()).isEqualTo("http://apicurio:8080");
    }

    @Test
    void cleanup_deletesConfigMapDeploymentService() {
        KafkaProxy proxy = proxy(null);

        reconciler.cleanup(proxy, context);

        verify(cmResource).delete();
        verify(namedDep).delete();
        verify(namedSvc).delete();
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
}
