package se.afshin.yavari.kafka.operator.ui;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.V1NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.ExternalAccessType;
import se.afshin.yavari.kafka.operator.crd.KafkaUI;
import se.afshin.yavari.kafka.operator.crd.KafkaUIOidcConfig;
import se.afshin.yavari.kafka.operator.crd.KafkaUISecretKeyRef;
import se.afshin.yavari.kafka.operator.crd.KafkaUISpec;
import se.afshin.yavari.kafka.operator.crd.KafkaUIStatus;
import se.afshin.yavari.kafka.operator.crd.McsConfig;

import java.util.List;
import se.afshin.yavari.kafka.operator.externalaccess.HttpExternalAccessConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpGatewayConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressBuilder;
import se.afshin.yavari.kafka.operator.externalaccess.HttpIngressConfig;
import se.afshin.yavari.kafka.operator.externalaccess.HttpRouteBuilder;
import se.afshin.yavari.kafka.operator.proxy.ExternalAccessResolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class KafkaUIReconcilerTest {

    private static final String NS = "kafka";
    private static final String UI_NAME = "kafka-ui";

    private KubernetesClient client;
    private UIDeploymentBuilder deploymentBuilder;
    private UIServiceBuilder serviceBuilder;
    private UIRbacBuilder rbacBuilder;
    private ExternalAccessResolver externalAccessResolver;
    private HttpIngressBuilder httpIngressBuilder;
    private HttpRouteBuilder httpRouteBuilder;
    private Context<KafkaUI> context;
    private KafkaUIReconciler reconciler;

    private RollableScalableResource depResource;
    private RollableScalableResource namedDep;
    private Resource ingResource;
    private Resource routeResource;
    private Resource serviceExportResource;
    private MixedOperation routeMixed;
    private NonNamespaceOperation routeNamespaced;

    @BeforeEach
    void setup() throws Exception {
        deploymentBuilder = mock(UIDeploymentBuilder.class);
        serviceBuilder = mock(UIServiceBuilder.class);
        rbacBuilder = mock(UIRbacBuilder.class);
        externalAccessResolver = new ExternalAccessResolver();
        httpIngressBuilder = mock(HttpIngressBuilder.class);
        httpRouteBuilder = mock(HttpRouteBuilder.class);
        context = mock(Context.class);
        client = mock(KubernetesClient.class);

        when(deploymentBuilder.build(any(), any())).thenReturn(deployment(1));
        when(serviceBuilder.build(any(), any())).thenReturn(new io.fabric8.kubernetes.api.model.Service());
        when(rbacBuilder.serviceAccount(any(), any())).thenReturn(new ServiceAccount());
        when(rbacBuilder.role(any(), any())).thenReturn(new RoleBuilder().build());
        when(rbacBuilder.roleBinding(any(), any())).thenReturn(new RoleBindingBuilder().build());
        when(httpIngressBuilder.build(anyString(), anyString(), any(), any(), anyString(),
                anyString(), anyInt(), any())).thenReturn(new Ingress());
        when(httpRouteBuilder.build(anyString(), anyString(), any(), any(), anyString(),
                anyString(), anyInt(), any())).thenReturn(new GenericKubernetesResource());

        // ServiceAccount chain
        MixedOperation saOp = mock(MixedOperation.class);
        NonNamespaceOperation nsSaOp = mock(NonNamespaceOperation.class);
        ServiceAccountResource saResource = mock(ServiceAccountResource.class);
        when(client.serviceAccounts()).thenReturn(saOp);
        when(saOp.inNamespace(NS)).thenReturn(nsSaOp);
        when(nsSaOp.resource(any(ServiceAccount.class))).thenReturn(saResource);

        // Rbac chain
        RbacAPIGroupDSL rbacApi = mock(RbacAPIGroupDSL.class);
        when(client.rbac()).thenReturn(rbacApi);
        MixedOperation roleOp = mock(MixedOperation.class);
        NonNamespaceOperation nsRoleOp = mock(NonNamespaceOperation.class);
        Resource roleResource = mock(Resource.class);
        when(rbacApi.roles()).thenReturn(roleOp);
        when(roleOp.inNamespace(NS)).thenReturn(nsRoleOp);
        when(nsRoleOp.resource(any(Role.class))).thenReturn(roleResource);
        MixedOperation rbOp = mock(MixedOperation.class);
        NonNamespaceOperation nsRbOp = mock(NonNamespaceOperation.class);
        Resource rbResource = mock(Resource.class);
        when(rbacApi.roleBindings()).thenReturn(rbOp);
        when(rbOp.inNamespace(NS)).thenReturn(nsRbOp);
        when(nsRbOp.resource(any(RoleBinding.class))).thenReturn(rbResource);

        // Deployments chain
        AppsAPIGroupDSL appsApi = mock(AppsAPIGroupDSL.class);
        when(client.apps()).thenReturn(appsApi);
        MixedOperation depOp = mock(MixedOperation.class);
        NonNamespaceOperation nsDepOp = mock(NonNamespaceOperation.class);
        depResource = mock(RollableScalableResource.class);
        namedDep = mock(RollableScalableResource.class);
        when(appsApi.deployments()).thenReturn(depOp);
        when(depOp.inNamespace(NS)).thenReturn(nsDepOp);
        when(nsDepOp.resource(any(Deployment.class))).thenReturn(depResource);
        when(nsDepOp.withName(UI_NAME)).thenReturn(namedDep);
        when(namedDep.get()).thenReturn(deploymentWithReadyReplicas(1));

        // Services chain
        MixedOperation svcOp = mock(MixedOperation.class);
        NonNamespaceOperation nsSvcOp = mock(NonNamespaceOperation.class);
        ServiceResource svcResource = mock(ServiceResource.class);
        when(client.services()).thenReturn(svcOp);
        when(svcOp.inNamespace(NS)).thenReturn(nsSvcOp);
        when(nsSvcOp.resource(any(io.fabric8.kubernetes.api.model.Service.class))).thenReturn(svcResource);
        // svcOp.withName used by the LB resolver
        when(nsSvcOp.withName(anyString())).thenReturn(svcResource);
        when(svcResource.get()).thenReturn(null);

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
        routeMixed = mock(MixedOperation.class);
        routeNamespaced = mock(NonNamespaceOperation.class);
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

        reconciler = new KafkaUIReconciler();
        injectField(reconciler, "client", client);
        injectField(reconciler, "deploymentBuilder", deploymentBuilder);
        injectField(reconciler, "serviceBuilder", serviceBuilder);
        injectField(reconciler, "rbacBuilder", rbacBuilder);
        injectField(reconciler, "externalAccessResolver", externalAccessResolver);
        injectField(reconciler, "httpIngressBuilder", httpIngressBuilder);
        injectField(reconciler, "httpRouteBuilder", httpRouteBuilder);
        injectField(reconciler, "localClusterId", "A");
        injectField(reconciler, "mcsEnabled", false);
    }

    @Test
    void reconcile_missingOidc_setsFailed() {
        KafkaUI ui = ui();
        ui.getSpec().setOidc(null);

        reconciler.reconcile(ui, context);

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.FAILED);
        assertThat(ui.getStatus().getMessage()).contains("spec.oidc is required");
        verify(rbacBuilder, never()).serviceAccount(any(), any());
    }

    @Test
    void reconcile_defaultNodePort_noIngressOrRoute() {
        KafkaUI ui = ui();

        reconciler.reconcile(ui, context);

        verify(deploymentBuilder, times(1)).build(any(), any());
        verify(serviceBuilder, times(1)).build(any(), any());
        // No Ingress / HTTPRoute built; both pruned.
        verify(httpIngressBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpRouteBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(ingResource, times(1)).delete();
        verify(routeResource, times(1)).delete();

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.READY);
        assertThat(ui.getStatus().getReadyReplicas()).isEqualTo(1);
    }

    @Test
    void reconcile_ingressType_appliesIngressOnly() {
        KafkaUI ui = ui();
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.INGRESS);
        ea.setAdvertisedHostTemplate("kafka-ui-${clusterId}.example.com");
        HttpIngressConfig ing = new HttpIngressConfig();
        ing.setIngressClassName("nginx");
        ea.setIngress(ing);
        ui.getSpec().setExternalAccess(ea);

        reconciler.reconcile(ui, context);

        verify(httpIngressBuilder, times(1)).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpRouteBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(routeResource, times(1)).delete();
        assertThat(ui.getStatus().getAdvertisedHost()).isEqualTo("kafka-ui-a.example.com");
    }

    @Test
    void reconcile_gatewayType_appliesRouteOnly() {
        KafkaUI ui = ui();
        HttpExternalAccessConfig ea = new HttpExternalAccessConfig();
        ea.setType(ExternalAccessType.GATEWAY);
        ea.setAdvertisedHostTemplate("kafka-ui.example.com");
        HttpGatewayConfig gw = new HttpGatewayConfig();
        gw.setParentGatewayName("external-gw");
        ea.setGateway(gw);
        ui.getSpec().setExternalAccess(ea);

        reconciler.reconcile(ui, context);

        verify(httpRouteBuilder, times(1)).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(httpIngressBuilder, never()).build(anyString(), anyString(), any(), any(),
                anyString(), anyString(), anyInt(), any());
        verify(ingResource, times(1)).delete();
        assertThat(ui.getStatus().getAdvertisedHost()).isEqualTo("kafka-ui.example.com");
    }

    @Test
    void reconcile_deploymentNotReady_reconciling() {
        when(namedDep.get()).thenReturn(deploymentWithReadyReplicas(0));
        KafkaUI ui = ui();

        reconciler.reconcile(ui, context);
        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.RECONCILING);
        assertThat(ui.getStatus().getReadyReplicas()).isEqualTo(0);
        assertThat(ui.getStatus().getMessage()).contains("Waiting for kafka-ui pods");
    }

    // ---- MCS multi-cluster HA (#20) ----

    @Test
    void mcsEnabled_clusterNotInTargetClusters_setsSkipped() {
        KafkaUI ui = ui();
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        ui.getSpec().setMcs(mcs);
        ui.getSpec().setTargetClusters(List.of("B", "C"));

        reconciler.reconcile(ui, context);

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.SKIPPED);
        assertThat(ui.getStatus().getMessage()).contains("'A'").contains("not a target");
        verify(deploymentBuilder, never()).build(any(), any());
        verify(serviceBuilder, never()).build(any(), any());
    }

    @Test
    void mcsEnabled_emptyTargetClusters_setsFailed() {
        KafkaUI ui = ui();
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        ui.getSpec().setMcs(mcs);
        ui.getSpec().setTargetClusters(List.of());

        reconciler.reconcile(ui, context);

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.FAILED);
        assertThat(ui.getStatus().getMessage()).contains("targetClusters");
        verify(deploymentBuilder, never()).build(any(), any());
    }

    @Test
    void mcsDisabled_butTargetClustersSet_setsFailed() {
        KafkaUI ui = ui();
        ui.getSpec().setTargetClusters(List.of("A"));

        reconciler.reconcile(ui, context);

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.FAILED);
        assertThat(ui.getStatus().getMessage()).contains("targetClusters").contains("mcs.enabled");
        verify(deploymentBuilder, never()).build(any(), any());
    }

    @Test
    void mcsEnabled_clusterInTargetClusters_appliesServiceExport() throws Exception {
        injectField(reconciler, "mcsEnabled", true); // operator-level MCS flag
        KafkaUI ui = ui();
        McsConfig mcs = new McsConfig();
        mcs.setEnabled(true);
        ui.getSpec().setMcs(mcs);
        ui.getSpec().setTargetClusters(List.of("A", "B", "C"));

        reconciler.reconcile(ui, context);

        assertThat(ui.getStatus().getPhase()).isEqualTo(KafkaUIStatus.Phase.READY);
        verify(deploymentBuilder, times(1)).build(any(), any());
        verify(serviceExportResource, times(1)).serverSideApply();
    }

    // ---- helpers ----

    private static int anyInt() {
        return org.mockito.ArgumentMatchers.anyInt();
    }

    private KafkaUI ui() {
        KafkaUI ui = new KafkaUI();
        ObjectMeta meta = new ObjectMeta();
        meta.setName(UI_NAME);
        meta.setNamespace(NS);
        meta.setUid("u");
        meta.setGeneration(1L);
        ui.setMetadata(meta);

        KafkaUISpec spec = new KafkaUISpec();
        KafkaUIOidcConfig oidc = new KafkaUIOidcConfig();
        oidc.setIssuerUrl("http://issuer");
        oidc.setClientId("kafka-ui-web");
        KafkaUISecretKeyRef ref = new KafkaUISecretKeyRef();
        ref.setName("kafka-ui-oidc");
        ref.setKey("client-secret");
        oidc.setClientSecretRef(ref);
        spec.setOidc(oidc);
        ui.setSpec(spec);
        return ui;
    }

    private Deployment deployment(int replicas) {
        return new DeploymentBuilder()
                .withNewMetadata().withName(UI_NAME).withNamespace(NS).endMetadata()
                .withNewSpec().withReplicas(replicas).endSpec()
                .build();
    }

    private Deployment deploymentWithReadyReplicas(int ready) {
        Deployment d = deployment(1);
        DeploymentStatus st = new DeploymentStatus();
        st.setReadyReplicas(ready);
        d.setStatus(st);
        return d;
    }

    private static void injectField(Object target, String name, Object value) throws Exception {
        var field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
