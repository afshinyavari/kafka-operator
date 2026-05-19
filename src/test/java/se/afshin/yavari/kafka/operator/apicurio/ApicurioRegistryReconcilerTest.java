package se.afshin.yavari.kafka.operator.apicurio;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class ApicurioRegistryReconcilerTest {

    private static final String NS = "kafka";
    private static final String REGISTRY_NAME = "my-apicurio";
    private static final String RBAC_REF = "my-rbac";

    private KubernetesClient client;
    private ApicurioDeploymentBuilder registryDeploymentBuilder;
    private ApicurioServiceBuilder registryServiceBuilder;
    private ApicurioProxyDeploymentBuilder proxyDeploymentBuilder;
    private ApicurioProxyServiceBuilder proxyServiceBuilder;
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

    @BeforeEach
    void setup() throws Exception {
        registryDeploymentBuilder = mock(ApicurioDeploymentBuilder.class);
        registryServiceBuilder = mock(ApicurioServiceBuilder.class);
        proxyDeploymentBuilder = mock(ApicurioProxyDeploymentBuilder.class);
        proxyServiceBuilder = mock(ApicurioProxyServiceBuilder.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        // ConfigMap chain (for RBAC policy check)
        cmOp = mock(MixedOperation.class);
        nsCmOp = mock(NonNamespaceOperation.class);
        namedCmOp = mock(Resource.class);
        when(client.configMaps()).thenReturn(cmOp);
        when(cmOp.inNamespace(NS)).thenReturn(nsCmOp);
        when(nsCmOp.withName(anyString())).thenReturn(namedCmOp);
        // default: policy CM exists
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

        // readyReplicas — registry deployment has 1 ready replica
        Deployment readyDep = new Deployment();
        DeploymentStatus ds = new DeploymentStatus();
        ds.setReadyReplicas(1);
        readyDep.setStatus(ds);
        when(namedDep.get()).thenReturn(readyDep);

        // Service chain
        svcOp = mock(MixedOperation.class);
        nsSvcOp = mock(NonNamespaceOperation.class);
        svcResource = mock(ServiceResource.class);
        namedSvc = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(Service.class))).thenReturn(svcResource);
        when(nsSvcOp.withName(anyString())).thenReturn(namedSvc);

        // Stubs
        when(registryDeploymentBuilder.build(any(), anyString())).thenReturn(new Deployment());
        when(registryServiceBuilder.build(any(), anyString())).thenReturn(new Service());
        when(proxyDeploymentBuilder.build(any(), anyString())).thenReturn(new Deployment());
        when(proxyServiceBuilder.build(any(), anyString())).thenReturn(new Service());

        reconciler = new ApicurioRegistryReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "registryDeploymentBuilder", registryDeploymentBuilder);
        injectField(reconciler, "registryServiceBuilder", registryServiceBuilder);
        injectField(reconciler, "proxyDeploymentBuilder", proxyDeploymentBuilder);
        injectField(reconciler, "proxyServiceBuilder", proxyServiceBuilder);
        injectField(reconciler, "mcsEnabled", false);
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
    void happyPath_noProxy_deploysRegistryOnly() {
        // No rbacProxyImage → proxy builders not called
        ApicurioRegistry registry = registry(null, null);

        reconciler.reconcile(registry, context);

        verify(registryDeploymentBuilder).build(any(), anyString());
        verify(registryServiceBuilder).build(any(), anyString());
        verify(proxyDeploymentBuilder, org.mockito.Mockito.never()).build(any(), anyString());
    }

    @Test
    void withProxy_deploysProxyDeploymentAndService() {
        // rbacRef + rbacProxyImage → proxy builders called
        ApicurioRegistry registry = registry(RBAC_REF, "proxy-image:latest");

        reconciler.reconcile(registry, context);

        verify(proxyDeploymentBuilder).build(any(), anyString());
        verify(proxyServiceBuilder).build(any(), anyString());
        // total: registry dep + proxy dep = 2 serverSideApply on deployments
        verify(depResource, times(2)).serverSideApply();
    }

    @Test
    void registryReady_setsRegistryUrl() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.reconcile(registry, context);

        assertThat(registry.getStatus().getRegistryUrl())
                .contains(REGISTRY_NAME + "-registry")
                .contains(NS)
                .contains(String.valueOf(ApicurioDeploymentBuilder.REGISTRY_PORT));
        assertThat(registry.getStatus().getPhase()).isEqualTo(ApicurioRegistryStatus.Phase.READY);
    }

    @Test
    void cleanup_deletesRegistryAndProxy() {
        ApicurioRegistry registry = registry(null, null);

        reconciler.cleanup(registry, context);

        verify(namedDep, times(2)).delete(); // registry + proxy deployments
        verify(namedSvc, times(2)).delete(); // registry + proxy services
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

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
